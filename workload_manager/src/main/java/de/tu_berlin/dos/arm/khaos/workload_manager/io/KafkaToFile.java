package de.tu_berlin.dos.arm.khaos.workload_manager.io;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class KafkaToFile implements Runnable {

    /******************************************************************************
     * STATIC VARIABLES
     ******************************************************************************/

    private static final Logger LOG = Logger.getLogger(KafkaToFile.class);

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    private final Properties props;
    private final String topic;
    private final String fileName;
    private final int timeLimit;
    private final StopWatch stopWatch;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public KafkaToFile(String brokerList, String topic, String fileName, int timeLimit) {

        // Kafka Consumer Properties
        this.props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", UUID.randomUUID().toString());
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.topic = topic;
        this.fileName = fileName;
        this.timeLimit = timeLimit;
        this.stopWatch = new StopWatch();
    }

    /******************************************************************************
     * INSTANCE BEHAVIOURS
     ******************************************************************************/

    public void run() {

        File output = new File(this.fileName);
        try {

            output.createNewFile();
        }
        catch (IOException e) {

            LOG.error(e);
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(output, true))) {

            int buffer = 0;
            //@SuppressWarnings("resource")
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(this.props);
            consumer.subscribe(Collections.singletonList(this.topic));

            this.stopWatch.start();
            while (this.stopWatch.getTime(TimeUnit.SECONDS) < this.timeLimit) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {

                    bw.append(record.value() + "\n");
                    buffer++;
                    if (buffer == 10000) {

                        buffer = 0;
                        bw.flush();
                    }
                }
            }
            this.stopWatch.reset();
        }
        catch (IOException ex) {

            LOG.error(ex);
        }
    }
}
