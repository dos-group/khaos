package de.tu_berlin.dos.arm.khaos.workload_manager;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public interface Consumers {

    interface Consumer {

        void execute() throws Exception;
    }

    class KafkaToFileConsumer implements Consumer {

        private final Properties props;
        private final String topic;
        private final int linesToFlush;
        private final File output;

        public KafkaToFileConsumer(String brokerList, String topic, int linesToFlush, File output) {

            // Kafka Consumer Properties
            this.props = new Properties();
            props.put("bootstrap.servers", brokerList);
            props.put("group.id", UUID.randomUUID().toString());
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("auto.offset.reset", "earliest");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            this.topic = topic;
            this.linesToFlush = linesToFlush;
            this.output = output;
        }

        @Override
        public void execute() throws Exception {

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(this.output, true))) {

                int buffer = 0;
                @SuppressWarnings("resource")
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(this.props);
                consumer.subscribe(Collections.singletonList(this.topic));
                while (true) {

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {

                        bw.append(record.value() + "\n");
                        buffer++;
                        if (buffer == this.linesToFlush) {

                            buffer = 0;
                            bw.flush();
                        }
                    }
                }
            }
        }
    }

}
