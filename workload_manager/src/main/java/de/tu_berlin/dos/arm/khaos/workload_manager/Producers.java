package de.tu_berlin.dos.arm.khaos.workload_manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonParser;
import de.tu_berlin.dos.arm.khaos.common.data.Event;
import de.tu_berlin.dos.arm.khaos.common.utils.DateUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public interface Producers {

    interface Producer {

    }

    /*************************************************************
     * Kafka Implementation Class                                *
     *************************************************************/

    class Kafka implements Producer {

        private static final Logger LOG = Logger.getLogger(Kafka.class);

        public static class EventSerializer implements Serializer<Event> {

            private static final Logger LOG = Logger.getLogger(EventSerializer.class);

            private final ObjectMapper objectMap = new ObjectMapper();

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) { }

            @Override
            public byte[] serialize(String topic, Event event) {
                try {
                    String msg = objectMap.writeValueAsString(event);
                    return msg.getBytes();
                }
                catch(JsonProcessingException ex) {
                    LOG.error("Error in Serialization", ex);
                }
                return null;
            }

            @Override
            public void close() { }
        }

        private final KafkaProducer<String, Event> kafkaProducer;

        public Kafka(String brokerList) {

            // configure kafka producer
            Properties kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers", brokerList);
            kafkaProps.put("acks", "0");
            kafkaProps.put("retries", 0);
            kafkaProps.put("batch.size", 16384);
            kafkaProps.put("linger.ms", 1000);
            kafkaProps.put("buffer.memory", 33554432);
            kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put("value.serializer", EventSerializer.class.getName());

            this.kafkaProducer = new KafkaProducer<>(kafkaProps);
        }
    }
}
