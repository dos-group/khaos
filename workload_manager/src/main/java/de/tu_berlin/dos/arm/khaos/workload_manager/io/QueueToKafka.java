package de.tu_berlin.dos.arm.khaos.workload_manager.io;

import de.tu_berlin.dos.arm.khaos.workload_manager.ReplayCounter;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueueToKafka implements Runnable {

    /******************************************************************************
     * STATIC VARIABLES
     ******************************************************************************/

    private static final Logger LOG = Logger.getLogger(QueueToKafka.class);

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    private final BlockingQueue<List<String>> queue;
    private final ReplayCounter replayCounter;
    private final String topic;
    private final KafkaProducer<String, String> kafkaProducer;
    private final AtomicBoolean isDone;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public QueueToKafka(
            BlockingQueue<List<String>> queue, ReplayCounter replayCounter,
            String topic, String brokerList, AtomicBoolean isDone) {

        this.queue = queue;
        this.replayCounter = replayCounter;
        this.topic = topic;
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", brokerList);
        kafkaProps.put("acks", "0");
        kafkaProps.put("retries", 0);
        kafkaProps.put("batch.size", 16384);
        kafkaProps.put("linger.ms", 1000);
        kafkaProps.put("buffer.memory", 33554432);
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.kafkaProducer = new KafkaProducer<>(kafkaProps);
        this.isDone = isDone;
    }

    /******************************************************************************
     * INSTANCE BEHAVIOURS
     ******************************************************************************/

    @Override
    public void run() {

        try {
            // wait till queue has items in it
            while (queue.isEmpty()) Thread.sleep(100);
            // initialize and start the stopwatch
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            // execute while file is still being read and queue is not empty
            while (!isDone.get() && !queue.isEmpty()) {
                // get current time
                int current = (int) stopWatch.getTime(TimeUnit.SECONDS);
                CompletableFuture.runAsync(() -> {
                    try {
                        //long startTime = System.currentTimeMillis();
                        queue.take().forEach(e -> kafkaProducer.send(new ProducerRecord<>(topic, e)));
                        //long endTime = System.currentTimeMillis();
                        //long timeNeeded =  endTime - startTime;
                        //LOG.info(timeNeeded);
                    }
                    catch (InterruptedException ex) {
                        LOG.error(ex);
                    }
                });
                // wait until second has passed before continuing next loop
                while (current < replayCounter.getCounter()) {

                    current = (int) stopWatch.getTime(TimeUnit.SECONDS);
                }
                replayCounter.incrementCounter();
            }
        }
        catch (InterruptedException ex) {

            LOG.error(ex);
        }
    }
}
