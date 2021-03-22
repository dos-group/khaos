package de.tu_berlin.dos.arm.khaos.workload_manager;

import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.Checkpoints;
import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.Vertices;
import de.tu_berlin.dos.arm.khaos.common.data.TimeSeries;
import de.tu_berlin.dos.arm.khaos.common.utils.DatasetSorter;
import de.tu_berlin.dos.arm.khaos.common.utils.FileParser;
import de.tu_berlin.dos.arm.khaos.common.utils.SequenceFSM;
import de.tu_berlin.dos.arm.khaos.workload_manager.Context.StreamingJob;
import de.tu_berlin.dos.arm.khaos.workload_manager.Context.StreamingJob.CheckpointSummary;
import de.tu_berlin.dos.arm.khaos.workload_manager.Context.StreamingJob.FailureMetrics;
import de.tu_berlin.dos.arm.khaos.workload_manager.ReplayCounter.Listener;
import de.tu_berlin.dos.arm.khaos.workload_manager.io.FileToQueue;
import de.tu_berlin.dos.arm.khaos.workload_manager.io.KafkaToFile;
import de.tu_berlin.dos.arm.khaos.workload_manager.io.QueueToKafka;
import de.tu_berlin.dos.arm.khaos.workload_manager.modeling.AnomalyDetector;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public enum ExecutionGraph implements SequenceFSM<Context, ExecutionGraph> {

    START {

        public ExecutionGraph runStage(Context context) {

            return ANALYZE;
        }
    },
    RECORD {

        public ExecutionGraph runStage(Context context) {

            // record files from kafka consumer topic to file for a user defined time
            KafkaToFile manager =
                new KafkaToFile(
                    context.brokerList,
                    context.consumerTopic,
                    context.originalFilePath,
                    context.timeLimit);
            manager.run();

            return SORT;
        }
    },
    SORT {

        public ExecutionGraph runStage(Context context) {

            // sort large events dataset and write to new file
            DatasetSorter.sort(
                context.originalFilePath,
                context.tempSortDir,
                context.sortedFilePath,
                context.tsLabel);

            return ANALYZE;
        }
    },
    ANALYZE {

        public ExecutionGraph runStage(Context context) throws Exception {

            // get the failure scenario by analysing the workload
            //WorkloadAnalyser analyser = WorkloadAnalyser.createFromEventsFile(context.sortedFilePath);
            //analyser.printWorkload(new File("workload.csv"));

            // TODO this gets a static set of values, to remove
            context.analyzer = WorkloadAnalyser.createFromWorkloadFile("workload.csv");
            List<Tuple2<Integer, Integer>> scenario =
                context.analyzer.getFailureScenario();
                    //context.minFailureInterval,
                    //context.averagingWindowSize,
                    //context.numFailures);

            scenario.forEach(System.out::println);
            System.out.println(scenario.size());

            return REGISTER;
        }
    },
    DEPLOY {

        public ExecutionGraph runStage(Context context) throws Exception {

            // deploy multiple experiments
            for (StreamingJob streamingJob : context.experiments) {

                String jobId =
                    context.flinkApiClient
                        .startJob(context.jarId, streamingJob.getProgramArgs(), context.parallelism)
                        .jobId;
                streamingJob.setJobId(jobId);

                // store list of operator ids
                List<Vertices.Node> vertices = context.flinkApiClient.getVertices(jobId).plan.nodes;
                ArrayList<String> operatorIds = new ArrayList<>();
                for (Vertices.Node vertex: vertices) {

                    operatorIds.add(vertex.id);
                    if (vertex.description.startsWith(context.sinkOperatorName)) {

                        streamingJob.setSinkId(vertex.id);
                    }
                }
                streamingJob.setOperatorIds(operatorIds);
            }
            return REGISTER;
        }
    },
    REGISTER {

        public ExecutionGraph runStage(Context context) throws Exception {

            // register points for failure injection with counter manager
            context.analyzer.getFailureScenario().forEach(point -> {

                context.replayCounter.register(new Listener(point, (avgThr) -> {

                    // inject failure in all experiments
                    for (StreamingJob streamingJob : context.experiments) {

                        try {
                            // measure avg latency based in averaging window
                            String query =
                                String.format(
                                    "sum(%s{job_id=\"%s\",quantile=\"0.95\",operator_id=\"%s\"})" +
                                    "/count(%s{job_id=\"%s\",quantile=\"0.95\",operator_id=\"%s\"})",
                                    context.latency, streamingJob.getJobId(), streamingJob.getSinkId(),
                                    context.latency, streamingJob.getJobId(), streamingJob.getSinkId());
                            long current = Instant.now().getEpochSecond();
                            TimeSeries tsLat =
                                context.prometheusApiClient.queryRange(
                                    query, current - context.averagingWindow, current);
                            double avgLat = tsLat.average();

                            // inject failure
                            String jobId = streamingJob.getJobId();
                            String operatorId = streamingJob.getOperatorIds().get(0);
                            String podName =
                                context.flinkApiClient
                                    .getTaskManagers(jobId, operatorId)
                                    .taskManagers.get(0).taskManagerId;
                            FailureInjector failureInjector = new FailureInjector();
                            failureInjector.crashFailure(podName, context.k8sNamespace);
                            failureInjector.client.close();

                            // read last checkpoint and calculate distance
                            long startTime = System.nanoTime();
                            long lastCheckpoint =
                                context
                                    .flinkApiClient.getCheckpoints(streamingJob.getJobId())
                                    .latest.completed.latestAckTimestamp;
                            long endTime = System.nanoTime();
                            long duration = (endTime - startTime);
                            // determine when the last checkpoint occurred taking request time into account
                            long chkLast = Instant.now().toEpochMilli() - lastCheckpoint - duration / 2;

                            // save gathered metrics
                            streamingJob.failureMetricsList.add(
                                new FailureMetrics(Instant.now().getEpochSecond(), avgThr, avgLat, chkLast));
                            LOG.info(streamingJob);
                        }
                        catch (Exception e) {

                            LOG.error(e.fillInStackTrace());
                        }
                    }
                }));
            });

            return MEASURE;
        }
    },
    REPLAY {

        public ExecutionGraph runStage(Context context) {

            // record start time of experiment
            StreamingJob.startTimestamp = Instant.now().getEpochSecond();

            // start generator
            CountDownLatch latch = new CountDownLatch(2);
            BlockingQueue<List<String>> queue = new ArrayBlockingQueue<>(60);
            AtomicBoolean isDone = new AtomicBoolean(false);
            // reset the replay counter to 1
            context.replayCounter.resetCounter();
            // start reading from file into queue and then into kafka
            CompletableFuture
                .runAsync(new FileToQueue(context.sortedFilePath, queue))
                .thenRun(() -> {
                    isDone.set(true);
                    latch.countDown();
                });
            CompletableFuture
                .runAsync(
                    new QueueToKafka(
                        queue,
                        context.replayCounter,
                        StreamingJob.consumerTopic,
                        context.brokerList,
                        isDone))
                .thenRun(latch::countDown);
            // wait till full workload has been replayed
            try {
                latch.await();
            }
            catch (InterruptedException e) {

                e.printStackTrace();
            }

            // record end time of experiment
            StreamingJob.stopTimestamp = Instant.now().getEpochSecond();

            LOG.info(context.experiments);

            return DELETE;
        }
    },
    DELETE {

        public ExecutionGraph runStage(Context context) throws Exception {

            for (StreamingJob streamingJob : context.experiments) {

                // save checkpoint metrics
                Checkpoints checkpoints = context.flinkApiClient.getCheckpoints(streamingJob.getJobId());
                streamingJob.setChkSummary(
                    new CheckpointSummary(
                        checkpoints.summary.endToEndDuration.min,
                        checkpoints.summary.endToEndDuration.avg,
                        checkpoints.summary.endToEndDuration.max,
                        checkpoints.summary.stateSize.min,
                        checkpoints.summary.stateSize.avg,
                        checkpoints.summary.stateSize.max));
                // Stop experimental job
                context.flinkApiClient.stopJob(streamingJob.getJobId());
            }
            return MEASURE;
        }
    },
    MEASURE {

        public ExecutionGraph runStage(Context context) throws Exception {

            int numOfCores = Runtime.getRuntime().availableProcessors();
            final ExecutorService service = Executors.newFixedThreadPool(numOfCores);
            final CountDownLatch latch = new CountDownLatch(context.numOfConfigs * context.numFailures);

            /*File thrFile = FileReader.GET.read("iot_8_thr_vehicles-cId5yU0Jb1.csv", File.class);
            File lagFile = FileReader.GET.read("iot_8_lag_vehicles-cId5yU0Jb1.csv", File.class);
            TimeSeries thrTs = FileParser.GET.fromCSV(thrFile, "\\|", true);
            TimeSeries lagTs = FileParser.GET.fromCSV(lagFile, "\\|", true);

            List<Long> failurePoints =
                Arrays.asList(
                    1615553479L, 1615575588L, 1615580781L, 1615594548L, 1615607816L,
                    1615612826L, 1615615276L, 1615616403L, 1615626120L, 1615630005L);

            for (Experiment experiment : context.experiments) {

                for (long failurePoint : failurePoints) {

                    experiment.failureMetricsList.add(new FailureMetrics(failurePoint, 0, 0,0));
                }
            }
            LOG.info(context.experiments);*/

            for (StreamingJob streamingJob : context.experiments) {

                // create queries for prometheus
                String queryThr =
                    String.format("sum(%s{job_id=\"%s\"})",
                        context.throughput, streamingJob.getJobId());
                String queryLag =
                    String.format("sum(%s{job_id=\"%s\"})/count(%s{job_id=\"%s\"})",
                        context.consumerLag, streamingJob.getJobId(),
                        context.consumerLag, streamingJob.getJobId());

                // query prometheus for metrics
                TimeSeries thrTs =
                    context.prometheusApiClient.queryRange(
                        queryThr, StreamingJob.startTimestamp, StreamingJob.stopTimestamp);
                TimeSeries lagTs =
                    context.prometheusApiClient.queryRange(
                        queryLag, StreamingJob.startTimestamp, StreamingJob.stopTimestamp);

                // TODO remove when tested
                String thrFileName =
                    String.format("%s_%s_%s_thr_results.csv",
                        context.jobName, context.parallelism, streamingJob.jobName);
                FileParser.GET.toCSV(thrFileName, thrTs,"timestamp|value", "\\|");
                String lagFileName =
                    String.format("%s_%s_%s_lag_results.csv",
                        context.jobName, context.parallelism, streamingJob.jobName);
                FileParser.GET.toCSV(lagFileName, lagTs,"timestamp|value", "\\|");

                for (FailureMetrics failureMetrics : streamingJob.failureMetricsList) {

                    service.submit(() -> {

                        AnomalyDetector detector = new AnomalyDetector(Arrays.asList(thrTs, lagTs));
                        detector.fit(failureMetrics.timestamp, 1000);
                        double duration = detector.measure(failureMetrics.timestamp, 600);
                        failureMetrics.setDuration(duration);
                        latch.countDown();
                    });
                }
            }

            latch.await();
            LOG.info(context.experiments);

            return MODEL;
        }
    },
    MODEL {

        public ExecutionGraph runStage(Context context) {

            // get values from experiments and fit models
            List<Double> durHats = new ArrayList<>();
            List<Double> avgLats = new ArrayList<>();
            List<Double> configs = new ArrayList<>();
            List<Double> avgThrs = new ArrayList<>();

            for (StreamingJob streamingJob : context.experiments) {

                // populate model dataset
                for (FailureMetrics failureMetrics : streamingJob.failureMetricsList) {

                    // test checkpoint interval and then normalize the recovery time
                    if (streamingJob.getConfig() > 5000) {

                        double chkDist = failureMetrics.timestamp - failureMetrics.chkLast;
                        if (chkDist < 0) chkDist = streamingJob.getConfig();
                        durHats.add(failureMetrics.getDuration() * (streamingJob.getConfig() / chkDist));
                    }
                    // dont worry about normalizing for these short checkpoint intervals
                    else durHats.add(streamingJob.getConfig());
                    avgLats.add(failureMetrics.avgLat);
                    configs.add(streamingJob.getConfig());
                    avgThrs.add(failureMetrics.avgThr);

                    // x_1 configs:     1000 1000 1000 1000 1000 14222 14222 14222 14222 14222
                    // x_2 throughputs: 486 24148 32276 12676 18647 486 24148 32276 12676 18647

                    // y latencies:   398 345 349 397 371 427 346 408 372 371 363

                    // y recovery:  time*dist/ci
                }
            }
            // fit models for performance and availability
            context.performance.fit(avgLats, Arrays.asList(configs, avgThrs));
            context.availability.fit(durHats, Arrays.asList(configs, avgThrs));

            // TODO update to GLS with Covariance Matrix

            // TODO get p-values to evaluate how good the model is

            return OPTIMIZE;
        }
    },
    OPTIMIZE {

        public ExecutionGraph runStage(Context context) throws Exception {

            final AtomicBoolean isWarmupTime = new AtomicBoolean(false);
            final StopWatch stopWatch = new StopWatch();
            while (true) {

                try {
                    stopWatch.start();
                    long current = stopWatch.getTime(TimeUnit.SECONDS);
                    long now = Instant.now().getEpochSecond();

                    // read last checkpoint and calculate distance
                    long startTime = System.nanoTime();
                    long lastCheckpoint =
                        context
                            .flinkApiClient.getCheckpoints(context.targetJob.getJobId())
                            .latest.completed.latestAckTimestamp;
                    long endTime = System.nanoTime();
                    long duration = (endTime - startTime);
                    // determine when the last checkpoint occurred taking request time into account
                    long chkLast = Instant.now().toEpochMilli() - lastCheckpoint - duration / 2;

                    // retrieve average latency
                    String queryLat =
                        String.format(
                            "sum(%s{job_id=\"%s\",quantile=\"0.95\",operator_id=\"%s\"})" +
                            "/count(%s{job_id=\"%s\",quantile=\"0.95\",operator_id=\"%s\"})",
                            context.latency, context.jobId, context.targetJob.getSinkId(),
                            context.latency, context.jobId, context.targetJob.getSinkId());
                    TimeSeries tsLat =
                        context.prometheusApiClient.queryRange(
                            queryLat, now - context.averagingWindow, now);
                    double avgLat = tsLat.average();

                    // retrieve average throughput
                    String queryThr =
                        String.format(
                            "sum(%s{job_id=\"%s\",quantile=\"0.95\",operator_id=\"%s\"})" +
                            "/count(%s{job_id=\"%s\",quantile=\"0.95\",operator_id=\"%s\"})",
                            context.throughput, context.jobId, context.targetJob.getSinkId(),
                            context.throughput, context.jobId, context.targetJob.getSinkId());
                    TimeSeries tsThr =
                        context.prometheusApiClient.queryRange(
                            queryThr, now - context.averagingWindow, now);
                    double avgThr = tsThr.average();

                    // TODO retrieve values from availability models
                    double recTime = 100;

                    // evaluate metrics based on constraints
                    if (context.constraintPerformance < avgLat && recTime < context.constraintAvailability) {

                        // TODO perform optimization step for performance

                    }
                    else if (context.constraintAvailability < recTime && avgLat < context.constraintPerformance) {

                        // TODO perform optimization step for availability

                    }
                    else if (context.constraintAvailability < recTime && context.constraintPerformance < avgLat) {

                        LOG.error(String.format("Unable to optimize, %s < %f and %d < %f",
                            context.constraintPerformance, avgLat, context.constraintAvailability, recTime));
                    }

                    // wait until next interval is reached
                    while (current < context.constraintInterval) {

                        current = stopWatch.getTime(TimeUnit.SECONDS);
                        Thread.sleep(100);
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

        public ExecutionGraph runStage(Context context) {

            return this;
        }
    };

    private static final Logger LOG = Logger.getLogger(ExecutionGraph.class);

    public static void start() throws Exception {

        LOG.info("START");
        ExecutionGraph.START.run(ExecutionGraph.class, Context.get);
    }
}
