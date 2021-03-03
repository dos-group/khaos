package de.tu_berlin.dos.arm.khaos.workload_manager;

import de.tu_berlin.dos.arm.khaos.common.utils.DatasetSorter;
import de.tu_berlin.dos.arm.khaos.common.utils.SequenceFSM;
import de.tu_berlin.dos.arm.khaos.workload_manager.Context.Experiment;
import de.tu_berlin.dos.arm.khaos.workload_manager.ReplayCounter.Listener;
import de.tu_berlin.dos.arm.khaos.workload_manager.io.FileToQueue;
import de.tu_berlin.dos.arm.khaos.workload_manager.io.KafkaToFile;
import de.tu_berlin.dos.arm.khaos.workload_manager.io.QueueToKafka;
import org.apache.log4j.Logger;
import scala.Tuple3;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public enum ExecutionGraph implements SequenceFSM<Context, ExecutionGraph> {

    START {

        public ExecutionGraph runStage(Context context) {

            LOG.info("START -> RECORD");
            return DEPLOY;
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
            return ANALYZE;
        }
    },
    ANALYZE {

        public ExecutionGraph runStage(Context context) {

            // get the failure scenario by analysing the workload
            WorkloadAnalyser analyser = WorkloadAnalyser.create(context.sortedFilePath);
            List<Tuple3<Integer, Timestamp, Integer>> scenario =
                analyser.getFailureScenario(10, 10, 5);
            scenario.forEach(System.out::println);


            // register points for failure injection with counter manager
            scenario.forEach(point -> {
                context.replayCounter.register(new Listener(point._1(), () -> {
                    // TODO measure avg latency

                    // TODO inject failure
                    // inject failure in all experiments
                    for (Experiment experiment : context.experiments) {

                        // inject failure
                        String podName = "flink-native-taskmanager-1-1";
                        FailureInjector failureInjector = new FailureInjector();
                        try {
                            failureInjector.crashFailure(podName, context.k8sNamespace);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        } catch (TimeoutException e) {
                            e.printStackTrace();
                        }
                        failureInjector.client.close();

                    }


                    LOG.info(point._1() + " " + point._2() + " " + point._3());
                }));
            });

            LOG.info("ANALYZE -> TRAIN");
            return TRAIN;
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

            }

            LOG.info("DEPLOY -> REPLAY");
            return DELETE;
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
                .runAsync(new QueueToKafka(queue, context.replayCounter, /*context.producerTopic*/context.consumerTopic, context.brokerList, isDone))
                .thenRun(latch::countDown);
            // wait till full workload has been replayed
            try {
                latch.await();
            }
            catch (InterruptedException e) {

                e.printStackTrace();
            }

            return STOP;
        }
    },
    DELETE {

        public ExecutionGraph runStage(Context context) throws Exception {

            Thread.sleep(1200000);

            for (Experiment experiment : context.experiments) {

                context.flinkApiClient.stopJob(experiment.getJobId());
            }

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
