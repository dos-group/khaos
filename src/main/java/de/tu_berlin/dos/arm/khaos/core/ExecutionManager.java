package de.tu_berlin.dos.arm.khaos.core;

import de.tu_berlin.dos.arm.khaos.clients.flink.responses.Checkpoints;
import de.tu_berlin.dos.arm.khaos.core.Context.Job;
import de.tu_berlin.dos.arm.khaos.io.IOManager.FailureMetrics;
import de.tu_berlin.dos.arm.khaos.io.IOManager.JobMetrics;
import de.tu_berlin.dos.arm.khaos.io.Observation;
import de.tu_berlin.dos.arm.khaos.io.ReplayCounter.Listener;
import de.tu_berlin.dos.arm.khaos.io.TimeSeries;
import de.tu_berlin.dos.arm.khaos.modeling.AnomalyDetector;
import de.tu_berlin.dos.arm.khaos.modeling.ForecastModel;
import de.tu_berlin.dos.arm.khaos.modeling.Optimization;
import de.tu_berlin.dos.arm.khaos.utils.SequenceFSM;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.Logger;
import scala.*;
import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.formula.Formula;
import smile.data.type.DataTypes;
import smile.data.type.StructField;
import smile.data.type.StructType;
import smile.regression.LinearModel;
import smile.regression.OLS;
import smile.regression.SVR;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.Double;
import java.lang.Long;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum ExecutionManager implements SequenceFSM<Context, ExecutionManager> {

    START {

        public ExecutionManager runStage(Context context) throws Exception {

            return RECORD;
        }
    },
    RECORD {

        public ExecutionManager runStage(Context context) {

            // saves events from kafka consumer topic to database for a user defined time
            if (context.doRecord) {

                context.IOManager.recordKafkaToDatabase(context.consumerTopic, context.timeLimit, 10000);
                context.IOManager.extractFullWorkload();
            }
            context.IOManager.extractFailureScenario(0.5f);
            return DEPLOY;
        }
    },
    DEPLOY {

        public ExecutionManager runStage(Context context) throws Exception {

            // deploy parallel profiling jobs
            for (Job job : context.experiment.jobs) {

                job.setJobId(context.clientsManager.startJob(job.getProgramArgs()));
                job.setOperatorIds(context.clientsManager.getOperatorIds(job.getJobId()));
                job.setSinkId(context.clientsManager.getSinkOperatorId(job.getJobId(), context.sinkRegex));
            }

            return REGISTER;
        }
    },
    REGISTER {

        public ExecutionManager runStage(Context context) throws Exception {

            context.IOManager.initFailureMetrics(context.experimentId, true);

            // register points for failure injection with counter manager
            context.IOManager.getFailureScenario().forEach(point -> {

                context.IOManager.registerListener(new Listener(point._1(), () -> {

                    for (Job job : context.experiment.jobs) {

                        try {
                            // wait until close to the end of checkpoint interval and then inject failure
                            while (true) {

                                long currTs = context.clientsManager.getLatestTs(job.getJobId());
                                long chkLast = context.clientsManager.getLastCheckpoint(job.getJobId());
                                // find time to point which is 3 seconds from when next checkpoint starts
                                long target = (job.config / 1000) - (currTs - chkLast) - context.chkTolerance;
                                if (target > 0) {
                                    context.executor.schedule(() -> {
                                        try {
                                            context.clientsManager.injectFailure(job.getJobId(), job.getSinkId());
                                            long startTs = currTs - context.averagingWindow;
                                            double avgThr = context.clientsManager.getThroughput(job.getJobId(), startTs, currTs).average();
                                            double avgLat = context.clientsManager.getLatency(job.getJobId(), job.getSinkId(), startTs, currTs).average();
                                            context.IOManager.addFailureMetrics(context.experimentId, job.getJobId(), currTs, avgThr, avgLat);
                                            for (FailureMetrics failureMetrics : context.IOManager.fetchFailureMetricsList(context.experimentId, job.getJobId())) {

                                                LOG.info(failureMetrics);
                                            }
                                            LOG.info("Finishing inject failure into job " + job.getJobId());
                                        }
                                        catch (Exception e) {

                                            e.printStackTrace();
                                            LOG.error("Failed to inject scheduled failure with message " + e.fillInStackTrace());
                                        }
                                    }, target, TimeUnit.SECONDS);
                                    break;
                                }
                                new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS);
                            }
                        }
                        catch (Exception e) {

                            LOG.error("Failed to inject failure with message " + e.fillInStackTrace());
                        }
                    }
                }));
            });
            return REPLAY;
        }
    },
    REPLAY {

        public ExecutionManager runStage(Context context) {

            // record start time of experiment
            context.experiment.setStartTs(Instant.now().getEpochSecond());

            BlockingQueue<List<String>> queue = new ArrayBlockingQueue<>(60);
            context.IOManager.getFailureScenario().forEach(point -> {
                // select range before and after failure point based on minInterval but smaller
                int startIndex = point._1() - context.minInterval * 2/3;
                int stopIndex = point._1() + context.minInterval * 2/3;

                AtomicBoolean isDone = new AtomicBoolean(false);
                CompletableFuture
                    .runAsync(context.IOManager.databaseToQueue(startIndex, stopIndex, queue))
                    .thenRun(() -> isDone.set(true));
                // wait till queue has items in it
                try { while (queue.isEmpty()) new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS); }
                catch(InterruptedException ex) { LOG.error(ex); }
                //
                context.IOManager.queueToKafka(startIndex, queue, context.experiment.consumerTopic, isDone).run();
            });
            // record end time of experiment
            context.experiment.setStopTs(Instant.now().getEpochSecond());

            return DELETE;
        }
    },
    DELETE {

        public ExecutionManager runStage(Context context) throws Exception {

            context.IOManager.initJobMetrics(context.experimentId, true);

            for (Job job : context.experiment.jobs) {

                // save checkpoint metrics
                Checkpoints checkpoints = context.clientsManager.flink.getCheckpoints(job.getJobId());
                context.IOManager.addJobMetrics(
                    context.experimentId,
                    job.getJobId(),
                    job.config,
                    checkpoints.summary.endToEndDuration.min,
                    checkpoints.summary.endToEndDuration.avg,
                    checkpoints.summary.endToEndDuration.max,
                    checkpoints.summary.stateSize.min,
                    checkpoints.summary.stateSize.avg,
                    checkpoints.summary.stateSize.max,
                    context.experiment.getStartTs(),
                    context.experiment.getStopTs());

                // Stop experimental job
                context.clientsManager.flink.stopJob(job.getJobId());
            }
            return MEASURE;
        }
    },
    MEASURE {

        public ExecutionManager runStage(Context context) throws Exception {

            int numOfCores = Runtime.getRuntime().availableProcessors();
            final ExecutorService executor = Executors.newFixedThreadPool(numOfCores);
            int total = context.numOfConfigs * context.numFailures;
            AtomicInteger counter = new AtomicInteger(0);
            final CountDownLatch latch = new CountDownLatch(total);

            LOG.info("Starting measure recovery times");
            for (JobMetrics jobMetrics : context.IOManager.fetchJobMetricsList(context.experimentId)) {

                TimeSeries thrTs = context.clientsManager.getThroughput(jobMetrics.jobId, jobMetrics.startTs, jobMetrics.stopTs);
                TimeSeries lagTs = context.clientsManager.getConsumerLag(jobMetrics.jobId, jobMetrics.startTs, jobMetrics.stopTs);

                for (FailureMetrics failureMetrics : context.IOManager.fetchFailureMetricsList(context.experimentId, jobMetrics.jobId)) {

                    executor.submit(() -> {

                        AnomalyDetector detector = new AnomalyDetector(Arrays.asList(thrTs, lagTs));
                        detector.fit(failureMetrics.timestamp, 1000);
                        double recTime = detector.measure(failureMetrics.timestamp);

                        LOG.info(failureMetrics.jobId + " " + failureMetrics.timestamp + " " + recTime);
                        context.IOManager.updateRecTime(failureMetrics.jobId, failureMetrics.timestamp, recTime);
                        LOG.info(counter.incrementAndGet() + "/" + total + " completed");
                        latch.countDown();
                    });
                }
            }
            latch.await();
            executor.shutdown();
            LOG.info("Finished measure recovery times");

            return MODEL;
        }
    },
    MODEL {

        public ExecutionManager runStage(Context context) {

            Map<Double, List<Tuple3<Double, Double, Double>>> perfMap = new TreeMap<>();
            Map<Double, List<Tuple3<Double, Double, Double>>> availMap= new TreeMap<>();
            for (int i = 1; i <= 5; i++) {

                for (JobMetrics jobMetrics : context.IOManager.fetchJobMetricsList(i)) {

                    perfMap.putIfAbsent(jobMetrics.config, new ArrayList<>());
                    availMap.putIfAbsent(jobMetrics.config, new ArrayList<>());

                    for (FailureMetrics failureMetrics : context.IOManager.fetchFailureMetricsList(i, jobMetrics.jobId)) {

                        perfMap.get(jobMetrics.config).add(Tuple3.apply(failureMetrics.avgThr, jobMetrics.config, failureMetrics.avgLat));
                        availMap.get(jobMetrics.config).add(Tuple3.apply(failureMetrics.avgThr, jobMetrics.config, failureMetrics.recTime));
                    }
                }
            }
            LOG.info(perfMap);
            LOG.info(availMap);

            List<Tuple3<Double, Double, Double>> perfAvgList = new ArrayList<>();
            for (Map.Entry<Double, List<Tuple3<Double, Double, Double>>> entry : perfMap.entrySet()) {

                int countThr = 0;
                double totalThr = 0;
                int countLat = 0;
                double totalLat = 0;
                for (int i = 0; i < entry.getValue().size(); i++) {

                    totalThr += entry.getValue().get(i)._1();
                    countThr++;
                    totalLat += entry.getValue().get(i)._3();
                    countLat++;
                }
                perfAvgList.add(new Tuple3<>(totalThr/countThr, entry.getKey(), totalLat/countLat));
            }
            LOG.info(Arrays.toString(perfAvgList.toArray()));

            List<Tuple3<Double, Double, Double>> availAvgList = new ArrayList<>();
            for (Map.Entry<Double, List<Tuple3<Double, Double, Double>>> entry : availMap.entrySet()) {

                int countThr = 0;
                double totalThr = 0;
                int countRecTime = 0;
                double totalRecTime = 0;
                for (int i = 0; i < entry.getValue().size(); i++) {

                    if (entry.getValue().get(i)._3() > 55) {

                        totalThr += entry.getValue().get(i)._1();
                        countThr++;
                        countRecTime += entry.getValue().get(i)._3();
                        totalRecTime++;
                    }

                }
                availAvgList.add(new Tuple3<>(totalThr/countThr, entry.getKey(), totalRecTime/countRecTime));
            }
            LOG.info(Arrays.toString(availAvgList.toArray()));

            LOG.info("-PERFORMANCE -----------------------------------------------------------------------");
            context.performance.fit(Tuple3.apply("thr", "conf", "lat"), perfAvgList, "lat");
            //LOG.info(context.performance.getModel());
            LOG.info(context.performance.predict("thr", 15000D, "conf", 30000D));

            LOG.info("-AVAILABILITY -----------------------------------------------------------------------");
            context.availability.fit(Tuple3.apply("thr", "conf", "recTime"), availAvgList, "recTime");
            //LOG.info(context.availability.getModel());
            LOG.info(context.availability.predict("thr", 15000D, "conf", 30000D));*/


            for (int i = 1; i <= 5; i++) {

                List<Tuple3<Double, Double, Double>> perfList = new ArrayList<>();
                List<Tuple3<Double, Double, Double>> availList = new ArrayList<>();
                for (JobMetrics jobMetrics : context.IOManager.fetchJobMetricsList(i)) {

                    for (FailureMetrics failureMetrics : context.IOManager.fetchFailureMetricsList(jobMetrics.experimentId, jobMetrics.jobId)) {

                        perfList.add(new Tuple3<>(failureMetrics.avgThr, jobMetrics.config, failureMetrics.avgLat));
                        if (failureMetrics.recTime > 55) availList.add(new Tuple3<>(failureMetrics.avgThr, jobMetrics.config, failureMetrics.recTime));
                    }
                }
                LOG.info("-PERFORMANCE " + i + " -----------------------------------------------------------------------");
                context.performance.fit(Tuple3.apply("thr", "conf", "lat"), perfList, "lat");
                LOG.info(Arrays.toString(context.performance.predict("thr", 15000D, "conf", 30000D)));

                LOG.info("-AVAILABILITY " + i + " -----------------------------------------------------------------------");
                context.availability.fit(Tuple3.apply("thr", "conf", "recTime"), availList, "recTime");
                LOG.info(Arrays.toString(context.availability.predict("thr", 15000D, "conf", 30000D)));
            }

            //-----------------------------------------------------------------------------------------

            Map<Double, List<List<Double>>> perfMap = new TreeMap<>();
            Map<Double, List<List<Double>>> availMap= new TreeMap<>();
            for (int i = 1; i <= 5; i++) {

                for (JobMetrics jobMetrics : context.IOManager.fetchJobMetricsList(i)) {

                    perfMap.putIfAbsent(jobMetrics.config, new ArrayList<>());
                    availMap.putIfAbsent(jobMetrics.config, new ArrayList<>());

                    for (FailureMetrics failureMetrics : context.IOManager.fetchFailureMetricsList(i, jobMetrics.jobId)) {

                        perfMap.get(jobMetrics.config).add(Arrays.asList(failureMetrics.avgThr, jobMetrics.config, failureMetrics.avgLat));
                        availMap.get(jobMetrics.config).add(Arrays.asList(failureMetrics.avgThr, jobMetrics.config, failureMetrics.recTime));
                    }
                }
            }

            int counter1 = 0, counter2 = 0;
            double [][] perfArr = new double[context.numOfConfigs * context.numFailures][];
            double [][] availArr = new double[context.numOfConfigs * context.numFailures - 7][];
            for (Map.Entry<Double, List<List<Double>>> entry : perfMap.entrySet()) {

                double key = entry.getKey();
                final int chunkSize = 5;

                final AtomicInteger count = new AtomicInteger(0);
                List<List<Double>> perfs = entry.getValue();
                perfs.sort(Comparator.comparing(o -> o.get(0)));
                final Collection<List<List<Double>>> perfRes =
                    perfs.stream()
                        .collect(Collectors.groupingBy(it -> count.getAndIncrement() / chunkSize))
                        .values();

                count.set(0);
                List<List<Double>> avails = availMap.get(key);
                avails.sort(Comparator.comparing(o -> o.get(0)));
                final Collection<List<List<Double>>> availRes =
                    avails.stream()
                        .collect(Collectors.groupingBy(it -> count.getAndIncrement() / chunkSize))
                        .values();

                Iterator<List<List<Double>>> it1 = perfRes.iterator();
                Iterator<List<List<Double>>> it2 = availRes.iterator();
                while (it1.hasNext()) {

                    List<List<Double>> currPerf = it1.next();
                    perfArr[counter1] = ArrayUtils.toPrimitive(currPerf.get(2).toArray(new Double[0]));
                    counter1++;
                    List<List<Double>> currAvail = it2.next();
                    double[] temp = ArrayUtils.toPrimitive(currAvail.get(2).toArray(new Double[0]));
                    if (temp[2] > 55) {

                        availArr[counter2] = temp;
                        counter2++;
                    }
                }
            }
            for (int i = 0; i < perfArr.length; i++) {

                System.out.println(Arrays.toString(perfArr[i]));
            }
            for (int i = 0; i < availArr.length; i++) {

                System.out.println(Arrays.toString(availArr[i]));
            }

            LOG.info("-PERFORMANCE -----------------------------------------------------------------------");
            context.performance.fit(Tuple3.apply("thr", "conf", "lat"), perfArr, "lat");
            //LOG.info(context.performance.getModel());
            LOG.info(Arrays.toString(context.performance.predict("thr", 15000D, "conf", 30000D)));

            LOG.info("-AVAILABILITY -----------------------------------------------------------------------");
            context.availability.fit(Tuple3.apply("thr", "conf", "recTime"), availArr, "recTime");
            //LOG.info(context.availability.getModel());
            LOG.info(Arrays.toString(context.availability.predict("thr", 15000D, "conf", 30000D)));

            return OPTIMIZE;
        }
    },
    OPTIMIZE {

        public ExecutionManager runStage(Context context) throws Exception {

            context.targetJob.setSinkId(context.clientsManager.getSinkOperatorId(context.jobId, context.sinkRegex));

            final StopWatch stopWatch = new StopWatch();
            while (true) {

                try {
                    // default
                    int optMultiplier = 1;
                    // ensure 
                    long uptime = context.clientsManager.getUptime(context.jobId);
                    if (uptime < context.minUpTime) {

                        new CountDownLatch(1).await((context.minUpTime - uptime), TimeUnit.SECONDS);
                        continue;
                    }

                    long stopTs = context.clientsManager.getLatestTs(context.targetJob.getJobId());
                    long startTs = stopTs - context.averagingWindow;

                    // fetch metrics and predict recovery time based on current throughput and checkpoint interval
                    double avgLat = context.clientsManager.getLatency(context.jobId, context.targetJob.getSinkId(), startTs, stopTs).average();
                    double avgThr = context.clientsManager.getThroughput(context.jobId, startTs, stopTs).average();
                    double recTime = context.availability.predict("thr", avgThr, "conf", context.targetJob.config)[1];

                    int windowMultiplier = 5;

                    // evaluate metrics based on constraints
                    if ((context.avgLatConst < avgLat  && context.recTimeConst >= recTime) ||
                        (context.avgLatConst >= avgLat && context.recTimeConst < recTime)) {

                        // Find range of valid checkpoint intervals
                        int size = (context.maxConfigVal - context.minConfigVal) / 1000;
                        double[] chkIntArr = new double[size];
                        for (int i = 0; i < size; i++) {

                            chkIntArr[i] = context.minConfigVal + (i * 1000);
                        }

                        // fetch average of workload per minute for the last hour
                        TimeSeries ts = context.clientsManager.getMsgInSec(context.consumerTopic, 60, stopTs - 3600, stopTs);
                        List<Double> filteredList = Arrays.stream(ts.values()).filter(c -> !Double.isNaN(c)).boxed().collect(Collectors.toList());
                        List<Double> filteredSubList = filteredList.subList(filteredList.size() % 60, filteredList.size());
                        int count = 0;
                        double total = 0;
                        double[] dataPoints = new double[filteredList.size() / 60];
                        for (int i = 0; i < filteredSubList.size(); i++) {

                            total += filteredSubList.get(i);
                            count++;

                            if (count == 60) {

                                dataPoints[i / 60] = total / count;
                                total = 0;
                                count = 0;

                            }
                        }
                        double lastDataPoint = dataPoints[dataPoints.length - 1];
                        // simple estimate of how throughput will evolve
                        double[] predThrs = context.forecast.fit(dataPoints).predict((int) Math.ceil(context.optInterval / 60));
                        // derive recommendation: is throughput evolving rapidly?
                        double sumOffPointWiseDifferences = ForecastModel.computeDifferences(lastDataPoint, predThrs);
                        boolean isUrgent = sumOffPointWiseDifferences < -0.1 * lastDataPoint;

                        if ((context.avgLatConst * context.maxViolation) < avgLat ||
                            (context.recTimeConst * context.maxViolation) < recTime || isUrgent){
                            // perform optimization, get best new checkpoint interval
                            double checkpointInterval = Optimization.getOptimalCheckpointInterval(context, avgThr, chkIntArr);
                            // TODO do something with the calculated new checkpoint interval
                            // we need to increase the multiplier, as we will have to wait longer
                            optMultiplier = 2;
                        }

                    }
                    else if (context.recTimeConst < recTime && context.avgLatConst < avgLat) {

                        LOG.warn(String.format(
                            "Unable to optimize, %s < %f and %d < %f",
                            context.avgLatConst, avgLat, context.recTimeConst, recTime));
                    }
                    else {

                        LOG.info("No violation");
                    }

                    // wait until next interval is reached
                    stopWatch.start();
                    long current = stopWatch.getTime(TimeUnit.SECONDS);
                    while (current < context.optInterval * optMultiplier) {

                        current = stopWatch.getTime(TimeUnit.SECONDS);
                        new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS);
                    }
                    stopWatch.reset();
                }
                catch (Exception e) {

                    LOG.error(e.fillInStackTrace());
                    break;
                }
            }
            return STOP;
        }
    },
    STOP {

        public ExecutionManager runStage(Context context) throws Exception {

            context.close();
            return this;
        }
    };

    private static final Logger LOG = Logger.getLogger(ExecutionManager.class);

    public static void start() throws Exception {

        LOG.info("START");
        ExecutionManager.START.run(ExecutionManager.class, Context.get);
    }
}
