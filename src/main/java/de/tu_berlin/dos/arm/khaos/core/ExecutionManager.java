package de.tu_berlin.dos.arm.khaos.core;

import de.tu_berlin.dos.arm.khaos.clients.flink.responses.Checkpoints;
import de.tu_berlin.dos.arm.khaos.core.Context.Job;
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

import java.lang.Double;
import java.lang.Long;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public enum ExecutionManager implements SequenceFSM<Context, ExecutionManager> {

    START {

        public ExecutionManager runStage(Context context) throws Exception {

            /*for (int i = 1; i <= 5; i++) {

                for (Tuple11<Integer, String, Double, Long, Long, Long, Long, Long, Long, Long, Long> job : context.IOManager.fetchJobs(i)) {

                    LOG.info(job);

                    for (Tuple6<Integer, String, Long, Double, Double, Double> current : context.IOManager.fetchMetrics(i, job._2())) {

                        LOG.info(current);
                    }
                }
            }*/
            long stopTs = Instant.now().getEpochSecond();
            long startTs = stopTs - context.averagingWindow;
            LOG.info(context.clientsManager.getMsgInSec("iot-vehicles-events", 10, startTs, stopTs));

            return STOP;
        }
    },
    RECORD {

        public ExecutionManager runStage(Context context) {

            // saves events from kafka consumer topic to database for a user defined time
            if (context.doRecord) {

                context.IOManager.recordKafkaToDatabase(context.consumerTopic, context.timeLimit, 10000);
                context.IOManager.extractFullWorkload();
            }
            context.IOManager.extractFailureScenario(0.2f);
            for (Tuple3<Integer, Long, Integer> current : context.IOManager.getFailureScenario()) {

                LOG.info(current._1() + " " + current._2() + " " + current._3());
            }
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

            context.IOManager.initMetrics(context.experimentId, true);

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
                                            context.IOManager.addMetrics(context.experimentId, job.getJobId(), currTs, avgThr, avgLat);
                                            for (Tuple6<Integer, String, Long, Double, Double, Double> current : context.IOManager.fetchMetrics(context.experimentId, job.getJobId())) {

                                                LOG.info(current);
                                            }
                                            LOG.info("Finishing inject failure into job " + job.getJobId());
                                        }
                                        catch (Exception e) {

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
            // TODO for baselines, can remove
            if (context.doReplayAll) {

                int startIndex = 0;
                int stopIndex = context.IOManager.getFullWorkload().size();
                AtomicBoolean isDone = new AtomicBoolean(false);
                CompletableFuture
                    .runAsync(context.IOManager.databaseToQueue(startIndex, stopIndex, queue))
                    .thenRun(() -> isDone.set(true));
                // wait till queue has items in it
                try { while (queue.isEmpty()) new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS); }
                catch(InterruptedException ex) { LOG.error(ex); }
                //
                context.IOManager.queueToKafka(startIndex, queue, context.experiment.consumerTopic, isDone).run();
            }
            else {

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
            }
            // record end time of experiment
            context.experiment.setStopTs(Instant.now().getEpochSecond());

            return DELETE;
        }
    },
    DELETE {

        public ExecutionManager runStage(Context context) throws Exception {

            context.IOManager.initJobs(context.experimentId, true);

            for (Job job : context.experiment.jobs) {

                // save checkpoint metrics
                Checkpoints checkpoints = context.clientsManager.flink.getCheckpoints(job.getJobId());
                context.IOManager.addJob(
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
            for (Tuple11<Integer, String, Double, Long, Long, Long, Long, Long, Long, Long, Long> job : context.IOManager.fetchJobs(context.experimentId)) {

                TimeSeries thrTs = context.clientsManager.getThroughput(job._2(), job._10(), job._11());
                TimeSeries lagTs = context.clientsManager.getConsumerLag(job._2(), job._10(), job._11());

                for (Tuple6<Integer, String, Long, Double, Double, Double> current : context.IOManager.fetchMetrics(context.experimentId, job._2())) {

                    executor.submit(() -> {

                        AnomalyDetector detector = new AnomalyDetector(Arrays.asList(thrTs, lagTs));
                        detector.fit(current._3(), 1000);
                        double recTime = detector.measure(current._3());

                        LOG.info(current._2() + " " + current._3() + " " + recTime);
                        context.IOManager.updateRecTime(current._2(), current._3(), recTime);
                        LOG.info(counter.incrementAndGet() + "/" + total + " completed");
                        latch.countDown();
                    });
                }
            }
            latch.await();
            executor.shutdown();
            LOG.info("Finished measure recovery times");

            return STOP;
        }
    },
    MODEL {

        public ExecutionManager runStage(Context context) {

            Map<Double, List<List<Double>>> perfMap = new TreeMap<>();
            Map<Double, List<List<Double>>> availMap= new TreeMap<>();
            for (int i = 1; i <= 5; i++) {

                for (Tuple11<Integer, String, Double, Long, Long, Long, Long, Long, Long, Long, Long> job : context.IOManager.fetchJobs(i)) {

                    perfMap.putIfAbsent(job._3(), new ArrayList<>());
                    availMap.putIfAbsent(job._3(), new ArrayList<>());

                    for (Tuple6<Integer, String, Long, Double, Double, Double> current : context.IOManager.fetchMetrics(i, job._2())) {

                        perfMap.get(job._3()).add(Arrays.asList(current._4(), job._3(), current._5()));
                        availMap.get(job._3()).add(Arrays.asList(current._4(), job._3(), current._6()));
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
            //LOG.info(Arrays.deepToString(perfArr));
            //LOG.info(Arrays.deepToString(availArr));

            /*double [][] perfArr = new double[perfList.size()][];
            double [][] availArr = new double[availList.size()][];
            for (int i = 0; i < perfList.size(); i++) {

                perfArr[i] = ArrayUtils.toPrimitive(perfList.get(i).toArray(new Double[0]));
                availArr[i] = ArrayUtils.toPrimitive(availList.get(i).toArray(new Double[0]));
            }
            LOG.info(Arrays.deepToString(perfArr));
            LOG.info(Arrays.deepToString(availArr));*/

            DataFrame dataDf = DataFrame.of(perfArr, "thr", "conf", "lat");
            LOG.info(dataDf);
            LinearModel model = OLS.fit(Formula.lhs("lat"), dataDf);
            LOG.info(model);
            LOG.info(Arrays.toString(model.coefficients()));

            double[][] tester = new double[1][];
            tester[0] = new double[]{15000D, 50000D};
            DataFrame test = DataFrame.of(tester, "thr", "conf");
            LOG.info(Arrays.toString(model.predict(test)));
            LOG.info(model.predict(new double[]{15000D, 50000D, 0D}));

            DataFrame dataDf2 = DataFrame.of(availArr, "recTime", "conf", "thr");
            LOG.info(dataDf2);
            LinearModel model2 = OLS.fit(Formula.lhs("recTime"), dataDf2);
            LOG.info(model2);
            LOG.info(Arrays.toString(model2.coefficients()));
            LOG.info(model2.predict(new double[]{0D, 50000D, 15000D}));

            // fit models for performance and availability
            //context.performance.fit(avgLats_apache, confThr_apache);
            //context.availability.fit(durHats, Arrays.asList(configs, avgThrs));
            //LOG.info(context.performance.predict(Arrays.asList(50000D, 30000D)));


            // Log R^2 and P-values for models
            //LOG.info(context.performance.calculateRSquared());
            //LOG.info(Arrays.toString(context.performance.calculatePValues().toArray()));
            //LOG.info(context.availability.calculateRSquared());
            //LOG.info(Arrays.toString(context.availability.calculatePValues().toArray()));

            return STOP;
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

                    long uptime = context.clientsManager.getUptime(context.jobId);
                    if (uptime < context.minUpTime) {

                        Thread.sleep((context.minUpTime - uptime) * 1000);
                        continue;
                    }

                    long stopTs = Instant.now().getEpochSecond();
                    long startTs = stopTs - context.averagingWindow;

                    // read current metrics
                    long chkLast = context.clientsManager.getLastCheckpoint(context.targetJob.getJobId());

                    double avgLat = context.clientsManager.getLatency(context.jobId, context.targetJob.getSinkId(), startTs, stopTs).average();
                    double avgThr = context.clientsManager.getThroughput(context.jobId, startTs, stopTs).average();
                    // TODO retrieve current checkpoint interval from flink
                    double currentCheckpointInterval = 10.0;
                    double recTime = context.availability.predict(Arrays.asList(currentCheckpointInterval, avgThr));

                    // TODO read checkpoint intervals from configuration?
                    double[] checkpointIntervals = new double[]{1.0, 2.0, 3.0};

                    // TODO how far do we want to look back?
                    int windowMultiplier = 5;

                    // evaluate metrics based on constraints
                    if ((context.avgLatConst < avgLat  && context.recTimeConst >= recTime) ||
                            (context.avgLatConst >= avgLat  && context.recTimeConst < recTime)) {

                        // resample to have less but meaningful data points
                        TimeSeries resampledPrevThr = context.clientsManager.
                                getThroughput(context.jobId, stopTs - (windowMultiplier * context.averagingWindow), stopTs)
                                .resample(60, java.util.Optional.empty());
                        double lastThr = resampledPrevThr.getLast().value;
                        // simple estimate of how throughput will evolve
                        double[] predThrs = context.forecast
                                .fit(resampledPrevThr.values())
                                .predict((int) Math.ceil(context.optInterval / 60));
                        // derive recommendation: is throughput evolving rapidly?
                        double sumOffPointwiseDifferences = ForecastModel.computeDifferences(lastThr, predThrs);
                        boolean isUrgent = sumOffPointwiseDifferences < -0.1 * lastThr;

                        if((context.avgLatConst * context.maxViolation) < avgLat ||
                                (context.recTimeConst * context.maxViolation) < recTime || isUrgent){
                            // perform optimization, get best new checkpoint interval
                            double checkpointInterval = Optimization
                                    .getOptimalCheckpointInterval(context, avgThr, checkpointIntervals);
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
