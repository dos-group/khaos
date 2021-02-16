package de.tu_berlin.dos.arm.khaos.workload_manager;

import de.tu_berlin.dos.arm.khaos.common.utils.FileReader;
import de.tu_berlin.dos.arm.khaos.workload_manager.CounterManager.CounterListener;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static de.tu_berlin.dos.arm.khaos.common.utils.DatasetSorter.sort;

public class Run {

    private static final Logger LOG = Logger.getLogger(Run.class);

    public static void main(String[] args) throws Exception {

        // get properties file
        Properties generatorProps = FileReader.GET.read("generator.properties", Properties.class);

        // retrieve dataset properties and create input file
        //String inputFileName = generatorProps.getProperty("dataset.inputFile");
        //String sortedFileName = generatorProps.getProperty("dataset.sortedFile");
        //sort(inputFileName, "/data/khaos/datasets/iot", sortedFileName, "ts");

        // retrieve kafka properties
        String topic = generatorProps.getProperty("kafka.topic");
        String brokerList = generatorProps.getProperty("kafka.brokerList");

        File file = new File("/data/khaos/datasets/iot_vehicles_events.csv");
        KafkaToFileManager manager = new KafkaToFileManager(brokerList, topic, 10000, file);
        manager.run();

        // perform analysis, retrieve max y value and create actors
        //WorkloadAnalyser workload = WorkloadAnalyser.create(inputFile);

        // create counter manager and register points for failure injection
        //CounterManager counterManager = new CounterManager();
        //counterManager.register(new CounterListener(10, () -> System.out.println("Callback for " + 10)));
        //counterManager.register(new CounterListener(60, () -> System.out.println("Callback for " + 60)));

        // start generator
        //CountDownLatch latch = new CountDownLatch(2);
        //BlockingQueue<List<String>> queue = new ArrayBlockingQueue<>(60);
        //AtomicBoolean isDone = new AtomicBoolean(false);

        // Run futures
        /*CompletableFuture
            .runAsync(new FileToQueueManager(inputFile, queue))
            .thenRun(() -> {
                isDone.set(true);
                latch.countDown();
            });
        CompletableFuture
            .runAsync(new QueueToKafkaManager(queue, counterManager, workload.getMaxY(), topic, brokerList, isDone))
            .thenRun(latch::countDown);

        // wait till workload has been replayed
        latch.await();*/

    }
}
