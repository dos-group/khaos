package de.tu_berlin.dos.arm.khaos.events;

import de.tu_berlin.dos.arm.khaos.events.ReplayCounter.Listener;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import scala.Tuple2;
import scala.Tuple3;

import javax.xml.stream.FactoryConfigurationError;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class EventsManager {

    /******************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************/

    @FunctionalInterface
    public interface CheckedConsumer<T> {

        void accept(T t) throws SQLException;
    }

    @FunctionalInterface
    public interface CheckedBiConsumer<T1, T2> {

        void accept(T1 t1, T2 t2) throws SQLException;
    }

    public static class DatabaseToQueue implements Runnable {

        public final List<Tuple3<Integer, Long, Integer>> workload;
        private final BlockingQueue<List<String>> queue;

        public DatabaseToQueue(List<Tuple3<Integer, Long, Integer>> workload, BlockingQueue<List<String>> queue) {

            this.workload = workload;
            this.queue = queue;
        }

        @Override
        public void run() {

            try (Connection conn = EventsManager.connect()) {

                LOG.info("Starting replay events from database to queue");
                for (Tuple3<Integer, Long, Integer> current : this.workload) {

                    String selectStatement = "SELECT body FROM events WHERE timestamp = " + current._2() + ";";
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
     * STATIC VARIABLES
     ******************************************************************************/

    private static final Logger LOG = Logger.getLogger(EventsManager.class);
    private static final String DB_FILE_NAME = "events_test";
    private static final StopWatch STOPWATCH = new StopWatch();

    /******************************************************************************
     * CLASS BEHAVIOURS
     ******************************************************************************/

    public static Connection connect() throws Exception {

        Class.forName("org.sqlite.JDBC");
        return DriverManager.getConnection(String.format("jdbc:sqlite:%s.db", DB_FILE_NAME));
    }

    public static void executeUpdate(String query) {

        try (Connection conn = connect();
             Statement statement = conn.createStatement()) {

            statement.executeUpdate(query);
        }
        catch (Exception e) {

            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    public static void executeQuery(String query, CheckedConsumer<ResultSet> callback) {

        try (Connection connection = connect();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            while (resultSet.next()) {

                callback.accept(resultSet);
            }
        }
        catch (Exception e) {

            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    public static <T> void executeQuery(String query, T target, CheckedBiConsumer<ResultSet, T> callback) {

        try (Connection connection = connect();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query)) {

            while (resultSet.next()) {

                callback.accept(resultSet, target);
            }
        }
        catch (Exception e) {

            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    private final ReplayCounter replayCounter;
    private final int minFailureInterval;
    private final int averagingWindow;
    private final int numFailures;
    private final Properties consumerProps;
    private final Properties producerProps;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public EventsManager(int minFailureInterval, int averagingWindow, int numFailures, String brokerList) {

        this.replayCounter = new ReplayCounter();
        this.minFailureInterval = minFailureInterval;
        this.averagingWindow = averagingWindow;
        this.numFailures = numFailures;

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
    }

    /******************************************************************************
     * INSTANCE BEHAVIOUR
     ******************************************************************************/

    public void recordKafkaToDatabase(String topic, int timeLimit, int bufferSize) {

        try (Connection conn = EventsManager.connect()) {

            EventsManager.executeUpdate("PRAGMA journal_mode=WAL;");
            EventsManager.executeUpdate("PRAGMA synchronous=off;");

            /*Statement statement = conn.createStatement();
            statement.executeUpdate("PRAGMA journal_mode=WAL;");
            statement.close();
            statement = conn.createStatement();
            statement.executeUpdate("PRAGMA synchronous=off;");
            statement.close();*/

            LOG.info("Starting create events table");
            String createTable =
                "CREATE TABLE IF NOT EXISTS events " +
                "(timestamp INTEGER NOT NULL, " +
                "body TEXT NOT NULL);";
            EventsManager.executeUpdate(createTable);
            LOG.info("Finished create events table");


            /*String createTable =
                "CREATE TABLE IF NOT EXISTS events " +
                "(timestamp INTEGER NOT NULL, " +
                "body TEXT NOT NULL);";
            statement = conn.createStatement();
            statement.executeUpdate(createTable);
            statement.close();
            String deleteRows = "DELETE FROM events;";
            statement = conn.createStatement();
            statement.executeUpdate(deleteRows);
            statement.close();
            LOG.info("Finished create events table");*/

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(this.consumerProps);
            consumer.subscribe(Collections.singletonList(topic));

            LOG.info("Starting record events");
            STOPWATCH.reset();
            STOPWATCH.start();
            while (STOPWATCH.getTime(TimeUnit.SECONDS) < timeLimit) {

                List<ConsumerRecords<String, String>> recordsList = new ArrayList<>();
                int buffer = 0;
                while (buffer < bufferSize) {

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
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
                EventsManager.executeUpdate(insertValue.append(";").toString());
                /*statement = conn.createStatement();
                statement.executeUpdate(insertValue.append(";").toString());
                statement.close();*/
            }
            LOG.info("Finished record events");

            LOG.info("Starting create index");
            String createIndex =
                "CREATE INDEX IF NOT EXISTS timestamp_index " +
                "ON events (timestamp);";
            EventsManager.executeUpdate(createIndex);
            /*statement = conn.createStatement();
            statement.executeUpdate(createIndex);
            statement.close();*/

            LOG.info("Finished create index");

            EventsManager.executeUpdate("PRAGMA synchronous=normal;");
            /*statement = conn.createStatement();
            statement.executeUpdate("PRAGMA synchronous=normal;");
            statement.close();*/
        }
        catch (Exception e) {

            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    public void extractWorkload() {

        //try (Connection conn = EventsManager.connect()) {

        LOG.info("Starting create workload table");
        String createTable =
            "CREATE TABLE IF NOT EXISTS workload " +
            "(second INTEGER NOT NULL, " +
            "timestamp INTEGER NOT NULL, " +
            "count INTEGER NOT NULL);";
        EventsManager.executeUpdate(createTable);
        /*Statement statement = conn.createStatement();
        statement.executeUpdate(createTable);
        statement.close();*/
        /*String deleteRows = "DELETE FROM workload;";
        statement = conn.createStatement();
        statement.executeUpdate(deleteRows);
        statement.close();*/
        EventsManager.executeUpdate("DELETE FROM workload;");
        LOG.info("Finished create workload table");

        LOG.info("Starting extract and insert workload");
        String selectEvents =
            "SELECT timestamp, COUNT(*) AS count " +
            "FROM events GROUP BY timestamp";
        AtomicInteger second = new AtomicInteger(1);
        EventsManager.executeQuery(selectEvents, (rs) -> {
            String insertValue = String.format(
                "INSERT INTO workload (second, timestamp, count) VALUES (%d,%d,%d);",
                second.getAndAdd(1), rs.getLong("timestamp"), rs.getInt("count"));
            EventsManager.executeUpdate(insertValue);
        });
        LOG.info("Finished extract and insert workload");

        /*statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(selectEvents);
        int second = 1;
        while (rs.next()) {

            statement = conn.createStatement();
            String insertValue = String.format(
                "INSERT INTO workload (second, timestamp, count) VALUES (%d,%d,%d);",
                second, rs.getLong("timestamp"), rs.getInt("count"));
            statement.executeUpdate(insertValue);
            statement.close();
            second++;
        }
        rs.close();
        statement.close();
        LOG.info("Finished extract workload");*/
        /*}
        catch (Exception e) {

            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }*/
    }

    public List<Tuple3<Integer, Long, Integer>> getWorkload() {

        List<Tuple3<Integer, Long, Integer>> workload = new ArrayList<>();

        LOG.info("Starting get workload");
        String selectValues =
            "SELECT second, timestamp, count " +
            "FROM workload ORDER BY timestamp ASC";
        EventsManager.executeQuery(selectValues, workload, (rs, target) -> {
            target.add(new Tuple3<>(rs.getInt("second"), rs.getLong("timestamp"), rs.getInt("count")));
        });
        LOG.info("Finished get workload");

        /*try (Connection conn = EventsManager.connect()) {

            LOG.info("Starting get workload");
            String sql =
                "SELECT second, timestamp, count " +
                "FROM workload ORDER BY timestamp ASC";
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {

                workload.add(new Tuple3<>(rs.getInt("second"), rs.getLong("timestamp"), rs.getInt("count")));
            }
            rs.close();
            statement.close();
            LOG.info("Finished get workload");
        }
        catch (Exception e) {

            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }*/
        return workload;
    }

    public void extractFailureScenario() {

        List<Tuple3<Integer, Long, Integer>> workload = this.getWorkload();
        LOG.info(workload.size());

        // find list of the max and minimum points
        List<Tuple3<Integer, Long, Integer>> min = new ArrayList<>();
        List<Tuple3<Integer, Long, Integer>> max = new ArrayList<>();
        for (int i = this.minFailureInterval; i < workload.size() - this.minFailureInterval; i++) {
            int sum = 0;
            for (int j = i - this.averagingWindow + 1; j <= i; j++) {
                sum += workload.get(j)._3();
            }
            int average = sum / this.averagingWindow;
            if (min.isEmpty()) min.add(workload.get(i));
            else if (min.get(0)._3() == average) min.add(workload.get(i));
            else if (average < min.get(0)._3()) {
                min.clear();
                min.add(workload.get(i));
            }
            if (max.isEmpty()) max.add(workload.get(i));
            else if (max.get(0)._3() == average) max.add(workload.get(i));
            else if (max.get(0)._3() < average) {
                max.clear();
                max.add(workload.get(i));
            }
        }
        // test if max and min were found
        if (min.size() == 0 || max.size() == 0) throw new IllegalStateException("Unable to find Max and/or Min intersects");
        // find values for average throughput where failures should be injected
        int maxVal = max.get(0)._3();
        int minVal = min.get(0)._3();
        int step = (int) (((maxVal - minVal) * 1.0 / (this.numFailures - 1)) + 0.5);
        // create map of for all points of intersection
        Map<Integer, List<Tuple3<Integer, Long, Integer>>> intersects = new TreeMap<>();
        intersects.put(minVal, min);
        Stream.iterate(minVal, i -> i + step).limit(this.numFailures - 1).forEach(i -> intersects.putIfAbsent(i, new ArrayList<>()));
        intersects.put(maxVal, max);
        // find all points of intersection in workload
        for (int i = this.minFailureInterval; i < workload.size() - this.minFailureInterval; i++) {

            int sum = 0;
            for (int j = i - this.averagingWindow + 1; j <= i; j++) {

                sum += workload.get(j)._3();
            }
            int average = sum / this.averagingWindow;
            // find range of values which is 1% of average
            int onePercent = (int) ((workload.size() / 100) + 0.5);
            List<Integer> targetRange =
                IntStream
                    .rangeClosed(average - onePercent , average + onePercent)
                    .boxed()
                    .collect(Collectors.toList());
            // find all intersects that are in 1% range of target
            for (int valueInRange : targetRange) {

                if (intersects.containsKey(valueInRange))

                    intersects.get(valueInRange).add(workload.get(i));
            }
        }
        LOG.info(intersects.size());
        // test if enough points were found at each intersect
        intersects.forEach((k,v) -> {
            if (v.size() == 0) throw new IllegalStateException("Unable to find intersect for " + k);
        });
        // randomly select combination of intersects where at least one full set exists
        List<Tuple3<Integer, Long, Integer>> scenario = new ArrayList<>();
        Random rand = new Random();
        StopWatch timer = new StopWatch();
        timer.start();
        while (true) {

            intersects.forEach((k,v) -> scenario.add(v.get(rand.nextInt(v.size()))));
            boolean valid = true;
            for (int i = 0; i < scenario.size(); i++) {
                for (int j = i+1; j < scenario.size(); j++) {
                    if (Math.abs(scenario.get(i)._3() - scenario.get(j)._3()) < this.minFailureInterval) {
                        valid = false;
                        break;
                    }
                }
            }
            if (valid) {

                try (Connection conn = EventsManager.connect()) {

                    String createTable =
                        "CREATE TABLE IF NOT EXISTS scenario " +
                        "(second INTEGER NOT NULL, " +
                        "timestamp INTEGER NOT NULL, " +
                        "count INTEGER NOT NULL);";
                    Statement statement = conn.createStatement();
                    statement.executeUpdate(createTable);
                    statement.close();
                    String deleteRows = "DELETE FROM scenario;";
                    statement = conn.createStatement();
                    statement.executeUpdate(deleteRows);
                    statement.close();

                    for (Tuple3<Integer, Long, Integer> current : scenario) {

                        statement = conn.createStatement();
                        String insertValue = String.format(
                            "INSERT INTO scenario (second, timestamp, count) VALUES (%d,%d,%d);",
                            current._1(), current._2(), current._3());
                        statement.executeUpdate(insertValue);
                        statement.close();
                    }
                }
                catch (Exception e) {

                    throw new IllegalStateException(e.getClass().getName() + ": " + e.getMessage());
                }
            }
            if (timer.getTime(TimeUnit.SECONDS) > 120)
                throw new IllegalStateException("Unable to find combination of intersects");
        }
    }

    public List<Tuple3<Integer, Long, Integer>> getFailureScenario() {

        List<Tuple3<Integer, Long, Integer>> scenario = new ArrayList<>();

        try (Connection conn = EventsManager.connect()) {

            LOG.info("Starting get scenario");
            String sql =
                "SELECT second, timestamp, count " +
                "FROM scenario ORDER BY second ASC";
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {

                scenario.add(new Tuple3<>(rs.getInt("second"), rs.getLong("timestamp"), rs.getInt("count")));
            }
            rs.close();
            statement.close();
            LOG.info("Finished get scenario");
        }
        catch (Exception e) {

            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }
        return scenario;
    }

    public void registerListener(Listener listener) {

        this.replayCounter.register(listener);
    }

    public DatabaseToQueue databaseToQueue(BlockingQueue<List<String>> queue) {

        return new DatabaseToQueue(this.getWorkload(), queue);
    }

    public QueueToKafka queueToKafka(BlockingQueue<List<String>> queue, String topic, AtomicBoolean isDone) {

        return new QueueToKafka(queue, this.replayCounter, this.producerProps, topic, isDone);
    }
}
