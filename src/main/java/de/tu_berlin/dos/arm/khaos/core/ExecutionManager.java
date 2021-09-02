package de.tu_berlin.dos.arm.khaos.core;

import de.tu_berlin.dos.arm.khaos.clients.flink.responses.Checkpoints;
import de.tu_berlin.dos.arm.khaos.clients.flink.responses.SaveStatus;
import de.tu_berlin.dos.arm.khaos.core.Context.Job;
import de.tu_berlin.dos.arm.khaos.io.IOManager.FailureMetrics;
import de.tu_berlin.dos.arm.khaos.io.IOManager.JobMetrics;
import de.tu_berlin.dos.arm.khaos.io.Observation;
import de.tu_berlin.dos.arm.khaos.io.ReplayCounter.Listener;
import de.tu_berlin.dos.arm.khaos.io.TimeSeries;
import de.tu_berlin.dos.arm.khaos.modeling.AnomalyDetector;
import de.tu_berlin.dos.arm.khaos.modeling.ForecastModel;
import de.tu_berlin.dos.arm.khaos.modeling.Optimization;
import de.tu_berlin.dos.arm.khaos.utils.LimitedQueue;
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
                                long target = (job.getConfig() / 1000) - (currTs - chkLast) - context.chkTolerance;
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
                    job.getConfig(),
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

                TimeSeries thrTs = context.clientsManager.getThroughput(jobMetrics.jobName, jobMetrics.startTs, jobMetrics.stopTs);
                TimeSeries lagTs = context.clientsManager.getConsumerLag(jobMetrics.jobName, jobMetrics.startTs, jobMetrics.stopTs);

                for (FailureMetrics failureMetrics : context.IOManager.fetchFailureMetricsList(context.experimentId, jobMetrics.jobName)) {

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

            List<Tuple3<Double, Double, Double>> perfList = new ArrayList<>();
            List<Tuple3<Double, Double, Double>> availList = new ArrayList<>();

            for (JobMetrics jobMetrics : context.IOManager.fetchJobMetricsList(context.experimentId)) {

                for (FailureMetrics failureMetrics : context.IOManager.fetchFailureMetricsList(context.experimentId, jobMetrics.jobName)) {

                    perfList.add(new Tuple3<>(failureMetrics.avgThr, jobMetrics.config, failureMetrics.avgLat));
                    if (failureMetrics.recTime > 55) {

                        availList.add(new Tuple3<>(failureMetrics.avgThr, jobMetrics.config, failureMetrics.recTime));
                    }
                }
            }

            double [][] perfArr = new double[perfList.size()][];
            for (int j = 0; j < perfList.size(); j++) {

                perfArr[j] = new double[]{perfList.get(j)._1(), perfList.get(j)._2(), perfList.get(j)._3()};
            }
            double [][] availArr = new double[availList.size()][];
            for (int j = 0; j < availList.size(); j++) {

                availArr[j] = new double[]{availList.get(j)._1(), availList.get(j)._2(), availList.get(j)._3()};
            }
            LOG.info("-PERFORMANCE -----------------------------------------------------------------------");
            context.performance.fit(Tuple3.apply("thr", "conf", "lat"), perfArr, "lat");

            LOG.info("-AVAILABILITY -----------------------------------------------------------------------");
            context.availability.fit(Tuple3.apply("thr", "conf", "recTime"), availArr, "recTime");

            return OPTIMIZE;
        }
    },
    OPTIMIZE {

        public ExecutionManager runStage(Context context) throws Exception {

            context.targetJob.setSinkId(context.clientsManager.getSinkOperatorId(context.jobId, context.sinkRegex));

            final StopWatch stopWatch = new StopWatch();
            int optMultiplier;
            Queue<Tuple2<Double, Double>> values = new LimitedQueue<>(6);
            while (true) {

                try {
                    // default
                    optMultiplier = 1;
                    // ensure
                    long uptime = context.clientsManager.getUptime(context.targetJob.getJobId());
                    LOG.info("uptime: " + uptime + ", chkInt: " + context.targetJob.getConfig());
                    if (uptime < context.minUpTime) {

                        new CountDownLatch(1).await((context.minUpTime - uptime), TimeUnit.SECONDS);
                    }

                    long stopTs = context.clientsManager.getLatestTs(context.targetJob.getJobId());
                    long startTs = stopTs - context.averagingWindow;
                    LOG.info("startTime: " + startTs);
                    LOG.info("stopTime: " + stopTs);

                    // fetch metrics and predict recovery time based on current throughput and checkpoint interval
                    double avgLat = context.clientsManager.getLatency(context.targetJob.jobName, context.targetJob.getSinkId(), startTs, stopTs).average();
                    double avgThr = context.clientsManager.getThroughput(context.targetJob.jobName, startTs, stopTs).average();
                    double recTime = context.availability.predict("thr", avgThr, "conf", context.targetJob.getConfig())[0];
                    double latency = context.performance.predict("thr", avgThr, "conf", context.targetJob.getConfig())[0];
                    LOG.info("recTime: " + recTime + ", avgLat: " + avgLat + ", predLat: " + latency + ", avgThr: " + avgThr);

                    // calculate rolling average of how far our model is out form the actual
                    values.add(new Tuple2<>(avgLat, latency));
                    double weight = values.stream().mapToDouble(i -> i._1 / i._2).sum() / (double) values.size();
                    LOG.info("weight: " + weight);

                    // Use time series forecasting to determine if change is urgently needed
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

                    if ((context.avgLatConst > avgLat && context.recTimeConst < recTime && isUrgent) ||
                            (context.avgLatConst < avgLat  && context.recTimeConst > recTime && isUrgent)) {

                        LOG.info("Violation Detected");

                        // Find range of valid checkpoint intervals
                        int size = (context.maxConfigVal - context.minConfigVal) / 1000;
                        double[] chkIntArr = new double[size];
                        for (int i = 0; i < size; i++) {

                            chkIntArr[i] = context.minConfigVal + (i * 1000);
                        }
                        double chkInt = -1;
                        // Process recovery time violation
                        if (context.avgLatConst > avgLat && context.recTimeConst < recTime) {

                            //chkInt = Optimization.optimize(OptType.RECTIME, context, avgThr, chkIntArr);
                            chkInt = Optimization.optimize("RECTIME", context, avgThr, chkIntArr, weight);
                        }
                        // process latency violation
                        else if (context.avgLatConst < avgLat && context.recTimeConst > recTime) {

                            //chkInt = Optimization.optimize(OptType.LATENCY, context, avgThr, chkIntArr);
                            chkInt = Optimization.optimize("LATENCY", context, avgThr, chkIntArr, weight);
                        }
                        LOG.info("difference: " + Math.abs(chkInt - context.targetJob.getConfig()));
                        // ensure valid checkpoint interval was found
                        if (chkInt != -1 && Math.abs((int) chkInt - context.targetJob.getConfig()) >= 5000) {

                            context.targetJob.setConfig((int) chkInt);
                            String requestId =
                                context.clientsManager.flink
                                    .saveJob(context.targetJob.getJobId(), true, context.savepoints)
                                    .requestId;
                            String savepointDir;
                            while (true) {

                                SaveStatus res = context.clientsManager.flink.checkStatus(context.targetJob.getJobId(), requestId);
                                LOG.info(res);
                                if (res.status != null && res.status.id.equalsIgnoreCase("COMPLETED")) {

                                    savepointDir = res.operation.location;
                                    LOG.info(res.operation.location);
                                    break;
                                }
                                new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS);
                            }
                            context.targetJob.setJobId(context.clientsManager.restartJob(savepointDir, context.targetJob.getProgramArgs()));
                            // we need to increase the multiplier, as we will have to wait longer
                            optMultiplier = 2;
                        }
                        else {
                            LOG.info("no better configuration was found");
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
