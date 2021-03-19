package de.tu_berlin.dos.arm.khaos.workload_manager;

import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.Checkpoints;
import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.Vertices;
import de.tu_berlin.dos.arm.khaos.common.data.Observation;
import de.tu_berlin.dos.arm.khaos.common.data.TimeSeries;
import de.tu_berlin.dos.arm.khaos.common.utils.DatasetSorter;
import de.tu_berlin.dos.arm.khaos.common.utils.FileParser;
import de.tu_berlin.dos.arm.khaos.common.utils.SequenceFSM;
import de.tu_berlin.dos.arm.khaos.workload_manager.Context.Experiment;
import de.tu_berlin.dos.arm.khaos.workload_manager.Context.Experiment.CheckpointSummary;
import de.tu_berlin.dos.arm.khaos.workload_manager.Context.Experiment.FailureMetrics;
import de.tu_berlin.dos.arm.khaos.workload_manager.ReplayCounter.Listener;
import de.tu_berlin.dos.arm.khaos.workload_manager.io.FileToQueue;
import de.tu_berlin.dos.arm.khaos.workload_manager.io.KafkaToFile;
import de.tu_berlin.dos.arm.khaos.workload_manager.io.QueueToKafka;
import de.tu_berlin.dos.arm.khaos.workload_manager.modeling.AnomalyDetector;
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
            for (Experiment experiment : context.experiments) {

                String jobId =
                    context.flinkApiClient
                        .startJob(context.jarId, experiment.getProgramArgs(), context.parallelism)
                        .jobId;
                experiment.setJobId(jobId);

                // store list of operator ids
                List<Vertices.Node> vertices = context.flinkApiClient.getVertices(jobId).plan.nodes;
                ArrayList<String> operatorIds = new ArrayList<>();
                for (Vertices.Node vertex: vertices) {

                    operatorIds.add(vertex.id);
                    if (vertex.description.startsWith(context.sinkOperatorName)) {

                        experiment.setSinkId(vertex.id);
                    }
                }
                experiment.setOperatorIds(operatorIds);
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
                    for (Experiment experiment : context.experiments) {

                        try {
                            // measure avg latency based in averaging window
                            String query =
                                String.format(
                                    "sum(%s{job_id=\"%s\",quantile=\"0.95\",operator_id=\"%s\"})" +
                                    "/count(%s{job_id=\"%s\",quantile=\"0.95\",operator_id=\"%s\"})",
                                    context.latency, experiment.getJobId(), experiment.getSinkId(),
                                    context.latency, experiment.getJobId(), experiment.getSinkId());
                            long current = Instant.now().getEpochSecond();
                            TimeSeries tsLat =
                                context.prometheusApiClient.queryRange(
                                    query, current - context.averagingWindow, current);
                            double sum = 0;
                            int count = 0;
                            for (int i = 0; i < tsLat.size(); i++) {

                                Observation observation = tsLat.observations.get(i);
                                if (!Double.isNaN(observation.value)) {

                                    sum += observation.value;
                                    count++;
                                }
                            }
                            double avgLat = sum / count;

                            // inject failure
                            String jobId = experiment.getJobId();
                            String operatorId = experiment.getOperatorIds().get(0);
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
                                    .flinkApiClient.getCheckpoints(experiment.getJobId())
                                    .latest.completed.latestAckTimestamp;
                            long endTime = System.nanoTime();
                            long duration = (endTime - startTime);
                            // determine when the last checkpoint occurred taking request time into account
                            long chkLast = Instant.now().toEpochMilli() - lastCheckpoint - duration / 2;

                            // save gathered metrics
                            experiment.failureMetricsList.add(
                                new FailureMetrics(Instant.now().getEpochSecond(), avgThr, avgLat, chkLast));
                            LOG.info(experiment);
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
            Experiment.startTimestamp = Instant.now().getEpochSecond();

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
                        Experiment.consumerTopic,
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
            Experiment.stopTimestamp = Instant.now().getEpochSecond();

            LOG.info(context.experiments);

            return DELETE;
        }
    },
    DELETE {

        public ExecutionGraph runStage(Context context) throws Exception {

            for (Experiment experiment : context.experiments) {

                // save checkpoint metrics
                Checkpoints checkpoints = context.flinkApiClient.getCheckpoints(experiment.getJobId());
                experiment.setChkSummary(
                    new CheckpointSummary(
                        checkpoints.summary.endToEndDuration.min,
                        checkpoints.summary.endToEndDuration.avg,
                        checkpoints.summary.endToEndDuration.max,
                        checkpoints.summary.stateSize.min,
                        checkpoints.summary.stateSize.avg,
                        checkpoints.summary.stateSize.max));
                // Stop experimental job
                context.flinkApiClient.stopJob(experiment.getJobId());
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

            for (Experiment experiment : context.experiments) {

                // create queries for prometheus
                String queryThr =
                    String.format("sum(%s{job_id=\"%s\"})",
                        context.throughput, experiment.getJobId());
                String queryLag =
                    String.format("sum(%s{job_id=\"%s\"})/count(%s{job_id=\"%s\"})",
                        context.consumerLag, experiment.getJobId(),
                        context.consumerLag, experiment.getJobId());

                // query prometheus for metrics
                TimeSeries thrTs =
                    context.prometheusApiClient.queryRange(
                        queryThr, Experiment.startTimestamp, Experiment.stopTimestamp);
                TimeSeries lagTs =
                    context.prometheusApiClient.queryRange(
                        queryLag, Experiment.startTimestamp, Experiment.stopTimestamp);

                // TODO remove when tested
                String thrFileName =
                    String.format("%s_%s_%s_thr_results.csv",
                        context.jobName, context.parallelism, experiment.jobName);
                FileParser.GET.toCSV(thrFileName, thrTs,"timestamp|value", "\\|");
                String lagFileName =
                    String.format("%s_%s_%s_lag_results.csv",
                        context.jobName, context.parallelism, experiment.jobName);
                FileParser.GET.toCSV(lagFileName, lagTs,"timestamp|value", "\\|");

                for (FailureMetrics failureMetrics : experiment.failureMetricsList) {

                    service.submit(() -> {

                        AnomalyDetector detector = new AnomalyDetector(Arrays.asList(thrTs, lagTs));
                        detector.fit(failureMetrics.timestamp, 1000);
                        double duration = detector.measure(failureMetrics.timestamp, 600);
                        failureMetrics.setRecDur(duration);
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
            List<Double> recDurs = new ArrayList<>();
            List<Double> avgLats = new ArrayList<>();
            List<Double> configs = new ArrayList<>();
            List<Double> avgThrs = new ArrayList<>();

            for (Experiment experiment : context.experiments) {

                // populate model dataset
                for (FailureMetrics failureMetrics : experiment.failureMetricsList) {

                    // test checkpoint interval and then normalize the recovery time
                    if (experiment.config > 5000) {

                        double chkDist = failureMetrics.getRecDur() - failureMetrics.chkLast;
                        if (chkDist > 0) chkDist = experiment.config;
                        recDurs.add(experiment.config / chkDist);
                    }
                    // dont worry about normalizing for these short checkpoint intervals
                    else recDurs.add(experiment.config);
                    avgLats.add(failureMetrics.avgLat);
                    configs.add(experiment.config);
                    avgThrs.add(failureMetrics.avgThr);

                }
            }
            // fit models for performance and availability
            context.performance.fit(avgLats, Arrays.asList(configs, avgThrs));
            context.availability.fit(recDurs, Arrays.asList(configs, avgThrs));

            return OPTIMIZE;
        }
    },
    OPTIMIZE {

        public ExecutionGraph runStage(Context context) {



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
