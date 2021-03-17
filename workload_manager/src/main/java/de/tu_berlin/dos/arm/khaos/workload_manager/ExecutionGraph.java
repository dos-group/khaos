package de.tu_berlin.dos.arm.khaos.workload_manager;

import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.Checkpoints;
import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.Vertices;
import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.responses.Matrix;
import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.responses.Vector;
import de.tu_berlin.dos.arm.khaos.common.utils.DatasetSorter;
import de.tu_berlin.dos.arm.khaos.common.utils.SequenceFSM;
import de.tu_berlin.dos.arm.khaos.workload_manager.Context.Experiment;
import de.tu_berlin.dos.arm.khaos.workload_manager.ReplayCounter.Listener;
import de.tu_berlin.dos.arm.khaos.workload_manager.io.FileToQueue;
import de.tu_berlin.dos.arm.khaos.workload_manager.io.KafkaToFile;
import de.tu_berlin.dos.arm.khaos.workload_manager.io.QueueToKafka;
import org.apache.log4j.Logger;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple6;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public enum ExecutionGraph implements SequenceFSM<Context, ExecutionGraph> {

    START {

        public ExecutionGraph runStage(Context context) {

            LOG.info("START -> RECORD");
            return RECORD;
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

            LOG.info("RECORD -> SORT");
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

            LOG.info("SORT -> EXTRACT");
            return STOP;
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

            LOG.info("ANALYZE -> TRAIN");
            return DEPLOY;
        }
    },
    TRAIN {

        public ExecutionGraph runStage(Context context) throws Exception {

            // TODO train anomaly detection model

            LOG.info("TRAIN -> DEPLOY");
            return DEPLOY;
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
                List<Vertices.Node> vertices =
                    context.flinkApiClient
                        .getVertices(jobId).plan.nodes;
                ArrayList<String> operatorIds = new ArrayList<>();
                for (Vertices.Node vertex: vertices) {
                    operatorIds.add(vertex.id);
                    if (vertex.description.startsWith(context.sinkOperatorName)) {
                        experiment.setSinkId(vertex.id);
                    }

                }
                experiment.setOperatorIds(operatorIds);

                // TODO temp measure to write to file
                File output = new File(experiment.jobName + ".log");
                if (!output.exists()) output.createNewFile();
                FileWriter fw = new FileWriter(experiment.jobName + ".log", true);
                fw.write(experiment.toString());
                fw.close();
            }

            // TODO remove
            context.experiments.forEach(System.out::println);

            LOG.info("DEPLOY -> REGISTER");
            return REGISTER;
        }
    },
    REGISTER {

        public ExecutionGraph runStage(Context context) throws Exception {

            // register points for failure injection with counter manager
            context.analyzer.getFailureScenario().forEach(point -> {

                context.replayCounter.register(new Listener(point, (throughput) -> {

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
                            Matrix matrix =
                                context.prometheusApiClient.queryRange(
                                    query, Instant.now().getEpochSecond() - context.averagingWindowSize + "",
                                    Instant.now().getEpochSecond() + "", "1", "120");
                            double sum = 0;
                            int count = 0;
                            for (int i = 0; i < matrix.data.result.get(0).values.size(); i++) {

                                sum += matrix.data.result.get(0).values.get(i).get(1);
                                count++;
                            }
                            double avgLatency = sum / count;

                            // read last checkpoint and calculate distance
                            Checkpoints checkpoints =
                                    context.flinkApiClient
                                            .getCheckpoints(experiment.getJobId());
                            long lastCheckpoint = checkpoints.latest.completed.latestAckTimestamp;
                            long checkpointDistance = Instant.now().toEpochMilli() - lastCheckpoint;

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

                            // save metrics
                            experiment.metrics.add(new Tuple4<>(Instant.now().getEpochSecond(), throughput, checkpointDistance, avgLatency));
                            experiment.checkpointSummary.add(new Tuple6<>(
                                    checkpoints.summary.endToEndDuration.min,
                                    checkpoints.summary.endToEndDuration.avg,
                                    checkpoints.summary.endToEndDuration.max,
                                    checkpoints.summary.stateSize.min,
                                    checkpoints.summary.stateSize.avg,
                                    checkpoints.summary.stateSize.max
                            ));
                            LOG.info(experiment);

                            // TODO remove
                            FileWriter fw = new FileWriter(experiment.jobName + ".log", true);
                            fw.write(experiment.toString());
                            fw.close();
                        }
                        catch (Exception e) {

                            LOG.error(e.fillInStackTrace());
                        }
                    }
                }));
            });

            LOG.info("REGISTER -> REPLAY");
            return REPLAY;
        }
    },
    REPLAY {

        public ExecutionGraph runStage(Context context) {

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

            return DELETE;
        }
    },
    DELETE {

        public ExecutionGraph runStage(Context context) throws Exception {

            //Thread.sleep(120000);

            for (Experiment experiment : context.experiments) {

                context.flinkApiClient.stopJob(experiment.getJobId());
            }

            LOG.info("DELETE -> END");
            return MEASURE;
        }
    },
    MEASURE {

        public ExecutionGraph runStage(Context context) {

            LOG.info("DELETE -> END");
            return MODEL;
        }
    },
    MODEL {

        public ExecutionGraph runStage(Context context) {

            LOG.info("DELETE -> END");
            return OPTIMIZE;
        }
    },
    OPTIMIZE {

        public ExecutionGraph runStage(Context context) {

            LOG.info("DELETE -> END");
            return STOP;
        }
    },
    STOP {

        public ExecutionGraph runStage(Context context) {

            LOG.info("STOP");
            return this;
        }
    };

    private static final Logger LOG = Logger.getLogger(ExecutionGraph.class);

    public static void start() throws Exception {

        LOG.info("START");
        ExecutionGraph.START.run(ExecutionGraph.class, Context.get);
    }
}
