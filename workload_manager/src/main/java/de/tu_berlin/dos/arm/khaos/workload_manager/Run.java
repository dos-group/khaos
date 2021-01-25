package de.tu_berlin.dos.arm.khaos.workload_manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.tu_berlin.dos.arm.khaos.common.data.Event;
import de.tu_berlin.dos.arm.khaos.common.utils.FileReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Map;
import java.util.Properties;

public class Run {

    private static final Logger LOG = Logger.getLogger(Run.class);

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

    public static void main(String[] args) throws Exception {

        // get properties file
        Properties generatorProps = FileReader.GET.read("generator.properties", Properties.class);

        // retrieve dataset file
        String fileName = generatorProps.getProperty("dataset.fileName");
        File file = FileReader.GET.read(fileName, File.class);

        // configure kafka producer
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", generatorProps.getProperty("kafka.brokerList"));
        kafkaProps.put("acks", "0");
        kafkaProps.put("retries", 0);
        kafkaProps.put("batch.size", 16384);
        kafkaProps.put("linger.ms", 1000);
        kafkaProps.put("buffer.memory", 33554432);
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", EventSerializer.class.getName());

        KafkaProducer<String, Event> kafkaProducer = new KafkaProducer<>(kafkaProps);


    }
}
