package de.tu_berlin.dos.arm.khaos.workload_manager;

import de.tu_berlin.dos.arm.khaos.common.utils.DatasetSorter;
import de.tu_berlin.dos.arm.khaos.common.utils.SequenceFSM;
import de.tu_berlin.dos.arm.khaos.workload_manager.ReplayCounter.Listener;
import de.tu_berlin.dos.arm.khaos.workload_manager.io.FileToQueue;
import de.tu_berlin.dos.arm.khaos.workload_manager.io.KafkaToFile;
import de.tu_berlin.dos.arm.khaos.workload_manager.io.QueueToKafka;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

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
        public ExecutionGraph runStage(Context context) {

            WorkloadAnalyser workload =
                WorkloadAnalyser.create(
                    context.sortedFilePath,
                    context.minTimeBetweenFailures,
                    context.numOfFailures);
            // TODO get the failure scenario from workload
            workload.getFailureScenario();
            // register points for failure injection with counter manager
            // TODO register the failure, and record the avg latency
            context.replayCounter.register(new Listener(10, () -> System.out.println("Callback for " + 10)));
            context.replayCounter.register(new Listener(60, () -> System.out.println("Callback for " + 60)));

            LOG.info("ANALYZE -> DEPLOY");
            return DEPLOY;
        }
    },

    DEPLOY {
        public ExecutionGraph runStage(Context context) {

            // TODO deploy multiple pipelines
            for (int i = 0; i < context.metricsNumOfConfigs; i++) {


            }
            //context.fli
            LOG.info("DEPLOY -> REPLAY");
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
                .runAsync(new QueueToKafka(queue, context.replayCounter, context.producerTopic, context.brokerList, isDone))
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

    public static void start() {

        LOG.info("START");
        ExecutionGraph.START.run(ExecutionGraph.class, Context.get);
    }
}
