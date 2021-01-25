package de.tu_berlin.dos.arm.khaos.workload_manager.producers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.tu_berlin.dos.arm.khaos.common.data.Event;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Properties;

public class Kafka {

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
