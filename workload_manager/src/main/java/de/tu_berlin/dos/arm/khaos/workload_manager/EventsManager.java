package de.tu_berlin.dos.arm.khaos.workload_manager;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventsManager {

    public static class DatabaseToQueue implements Runnable {

        private final List<Tuple2<Long, Integer>> workload;
        private final BlockingQueue<List<String>> queue;

        public DatabaseToQueue(List<Tuple2<Long, Integer>> workload, BlockingQueue<List<String>> queue) {

            this.workload = workload;
            this.queue = queue;
        }

        @Override
        public void run() {

            try (Connection conn = EventsManager.connect()) {

                LOG.info("Starting replay events from database to queue");
                for (Tuple2<Long, Integer> current : this.workload) {

                    String selectStatement = "SELECT body FROM events WHERE timestamp = " + current._1 + ";";
                    Statement statement = conn.createStatement();
                    ResultSet rs = statement.executeQuery(selectStatement);
                    List<String> events = new ArrayList<>();
                    while (rs.next()) {

                        events.add(rs.getString("body"));
                    }
                    queue.put(events);
                    rs.close();
                    statement.close();
                }
                LOG.info("Stopping replay events from database to queue");
            }
            catch (Exception e) {

                LOG.error(e.getClass().getName() + ": " + e.getMessage());
            }
        }
    }

    public static class QueueToKafka implements Runnable {

        private static final Logger LOG = Logger.getLogger(QueueToKafka.class);

        private final BlockingQueue<List<String>> queue;
        private final ReplayCounter replayCounter;
        private final String producerTopic;
        private final KafkaProducer<String, String> kafkaProducer;
        private final AtomicBoolean isDone;

        public QueueToKafka(
                BlockingQueue<List<String>> queue, ReplayCounter replayCounter,
                Properties producerProps, String producerTopic, AtomicBoolean isDone) {

            this.queue = queue;
            this.replayCounter = replayCounter;
            this.producerTopic = producerTopic;
            this.kafkaProducer = new KafkaProducer<>(producerProps);
            this.isDone = isDone;
        }

        @Override
        public void run() {

            LOG.info("Starting replay events from queue to kafka");
            try {
                // wait till queue has items in it
                while (queue.isEmpty()) Thread.sleep(100);
                // initialize and start the stopwatch
                STOPWATCH.reset();
                STOPWATCH.start();
                // execute while file is still being read and queue is not empty
                while (!isDone.get() && !queue.isEmpty()) {
                    // get current time
                    int current = (int) STOPWATCH.getTime(TimeUnit.SECONDS);
                    CompletableFuture.runAsync(() -> {
                        try {
                            //long startTime = System.currentTimeMillis();
                            List<String> events = queue.take();
                            events.forEach(e -> kafkaProducer.send(new ProducerRecord<>(producerTopic, e)));
                            //long endTime = System.currentTimeMillis();
                            //long timeNeeded =  endTime - startTime;
                            LOG.info(replayCounter.getCounter() + " " + events.size());
                        }
                        catch (InterruptedException ex) {
                            LOG.error(ex);
                        }
                    });
                    // wait until second has passed before continuing next loop
                    while (current < replayCounter.getCounter()) {

                        current = (int) STOPWATCH.getTime(TimeUnit.SECONDS);
                    }
                    replayCounter.incrementCounter();
                }
            }
            catch (InterruptedException ex) {

                LOG.error(ex);
            }
            LOG.info("Stopping replay events from database to queue");
        }
    }

    /******************************************************************************
     * CLASS VARIABLES
     ******************************************************************************/

    private static final Logger LOG = Logger.getLogger(EventsManager.class);
    private static final String DB_FILE_NAME = "events";
    private static final StopWatch STOPWATCH = new StopWatch();

    /******************************************************************************
     * CLASS BEHAVIOURS
     ******************************************************************************/

    private static Connection connect() throws Exception {

        Class.forName("org.sqlite.JDBC");
        return DriverManager.getConnection(String.format("jdbc:sqlite:%s.db", DB_FILE_NAME));
    }

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    private final Properties consumerProps;
    private final String consumerTopic;
    private final Properties producerProps;
    private final String producerTopic;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public EventsManager(String brokerList, String consumerTopic, String producerTopic) {

        // Kafka Consumer Properties
        this.consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", brokerList);
        consumerProps.put("group.id", UUID.randomUUID().toString());
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("auto.offset.reset", "latest");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.consumerTopic = consumerTopic;

        // Kafka Producer Properties
        this.producerProps = new Properties();
        producerProps.put("bootstrap.servers", brokerList);
        producerProps.put("acks", "0");
        producerProps.put("retries", 0);
        producerProps.put("batch.size", 16384);
        producerProps.put("linger.ms", 1000);
        producerProps.put("buffer.memory", 33554432);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.producerTopic = producerTopic;
    }

    /******************************************************************************
     * INSTANCE BEHAVIOUR
     ******************************************************************************/

    public void recordKafkaToDatabase(int timeLimit, int bufferSize) {

        try (Connection conn = EventsManager.connect()) {

            Statement statement = conn.createStatement();
            statement.executeUpdate("PRAGMA journal_mode=WAL;");
            statement.close();
            statement = conn.createStatement();
            statement.executeUpdate("PRAGMA synchronous=off;");
            statement.close();

            String createTable =
                "CREATE TABLE IF NOT EXISTS events " +
                "(timestamp INTEGER NOT NULL, " +
                "body TEXT NOT NULL);";
            statement = conn.createStatement();
            statement.executeUpdate(createTable);
            statement.close();

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(this.consumerProps);
            consumer.subscribe(Collections.singletonList(this.consumerTopic));

            STOPWATCH.reset();
            STOPWATCH.start();
            while (STOPWATCH.getTime(TimeUnit.SECONDS) < timeLimit) {

                List<ConsumerRecords<String, String>> recordsList = new ArrayList<>();
                int buffer = 0;
                while (buffer < bufferSize) {

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    if (records.count() > 0) {

                        buffer += records.count();
                        recordsList.add(records);
                    }
                }
                StringBuilder insertValue = new StringBuilder().append("INSERT INTO events (timestamp, body) VALUES ");
                Iterator<ConsumerRecords<String, String>> listIterator = recordsList.iterator();
                while (listIterator.hasNext()) {

                    ConsumerRecords<String, String> records = listIterator.next();
                    Iterator<ConsumerRecord<String, String>> recordsIterator = records.iterator();
                    while (recordsIterator.hasNext()) {

                        ConsumerRecord<String, String> record = recordsIterator.next();
                        insertValue.append(String.format("(%d,'%s')", record.timestamp(), record.value()));
                        if (recordsIterator.hasNext()) insertValue.append(",");
                    }
                    if (listIterator.hasNext()) insertValue.append(",");
                }
                statement = conn.createStatement();
                statement.executeUpdate(insertValue.append(";").toString());
                statement.close();
            }
            LOG.info("Starting Create Index");
            String createIndex =
                "CREATE INDEX IF NOT EXISTS timestamp_index " +
                "ON events (timestamp);";
            statement = conn.createStatement();
            statement.executeUpdate(createIndex);
            statement.close();
            LOG.info("Finished Create Index");
        }
        catch (Exception e) {

            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    public List<Tuple2<Long, Integer>> getWorkload() {

        List<Tuple2<Long, Integer>> workload = new ArrayList<>();

        try (Connection conn = EventsManager.connect()) {

            String sql =
                "SELECT timestamp, COUNT(*) AS count " +
                "FROM events GROUP BY timestamp";
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {

                workload.add(new Tuple2<>(rs.getLong("timestamp"), rs.getInt("count")));
            }
            rs.close();
            statement.close();
        }
        catch (Exception e) {

            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }
        return workload;
    }

    public DatabaseToQueue databaseToQueue(BlockingQueue<List<String>> queue) {

        return new DatabaseToQueue(this.getWorkload(), queue);
    }

    public QueueToKafka queueToKafka(BlockingQueue<List<String>> queue, ReplayCounter replayCounter, AtomicBoolean isDone) {

        return new QueueToKafka(queue, replayCounter, this.producerProps, this.producerTopic, isDone);
    }
}
