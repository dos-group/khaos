package de.tu_berlin.dos.arm.khaos.io;

import de.tu_berlin.dos.arm.khaos.io.ReplayCounter.Listener;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import scala.*;

import java.lang.Double;
import java.lang.Long;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class IOManager {

    /******************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************/

    @FunctionalInterface
    public interface CheckedConsumer<T> {

        void accept(T t) throws SQLException;
    }

    public static class DatabaseToQueue implements Runnable {

        private static final Logger LOG = Logger.getLogger(DatabaseToQueue.class);

        private final List<Tuple3<Integer, Long, Integer>> currWorkload;
        private final BlockingQueue<List<String>> queue;

        public DatabaseToQueue(List<Tuple3<Integer, Long, Integer>> currWorkload, BlockingQueue<List<String>> queue) {

            this.currWorkload = currWorkload;
            this.queue = queue;
        }

        @Override
        public void run() {

            LOG.info("Starting replay events from database to queue");
            for (Tuple3<Integer, Long, Integer> current : this.currWorkload) {

                List<String> events = new ArrayList<>();
                String selectValue = "SELECT body FROM events WHERE timestamp = " + current._2() + ";";
                IOManager.executeQuery(selectValue, (rs) -> events.add(rs.getString("body")));
                try {
                    queue.put(events);
                }
                catch (InterruptedException e) {

                    LOG.error(e.getClass().getName() + ": " + e.getMessage());
                }
            }
            LOG.info("Stopping replay events from database to queue");
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

            LOG.info("Starting replay events from queue to kafka topic " + this.producerTopic);

            // execute while file is still being read and queue is not empty
            while (!isDone.get() && !queue.isEmpty()) {
                // initialize and start the stopwatch
                STOPWATCH.reset();
                STOPWATCH.start();
                // get current time
                long current = STOPWATCH.getTime(TimeUnit.MILLISECONDS);
                CompletableFuture.runAsync(() -> {
                    try {
                        List<String> events = queue.take();
                        events.forEach(e -> kafkaProducer.send(new ProducerRecord<>(producerTopic, e)));
                    }
                    catch (Exception ex) { LOG.error(ex); }
                });
                // wait until second has passed before continuing next loop
                while (current < 1000) {

                    current = STOPWATCH.getTime(TimeUnit.MILLISECONDS);
                }
                replayCounter.incrementCounter();
            }
            LOG.info("Starting replay events from queue to kafka topic " + this.producerTopic);
        }
    }

    /******************************************************************************
     * STATIC VARIABLES
     ******************************************************************************/

    private static final Logger LOG = Logger.getLogger(IOManager.class);
    private static final String DB_FILE_NAME = "events";
    private static final Random RANDOM = new Random();
    private static final StopWatch STOPWATCH = new StopWatch();

    /******************************************************************************
     * STATIC BEHAVIOURS
     ******************************************************************************/

    private static Connection connect() throws Exception {

        Class.forName("org.sqlite.JDBC");
        return DriverManager.getConnection(String.format("jdbc:sqlite:%s.db", DB_FILE_NAME));
    }

    private static void executeUpdate(String query) {

        try (Connection conn = connect();
             Statement statement = conn.createStatement()) {

            statement.executeUpdate(query);
        }
        catch (Exception e) {

            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    private static void executeQuery(String query, CheckedConsumer<ResultSet> callback) {

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

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    private final ReplayCounter replayCounter;
    private final int minInterval;
    private final int averagingWindow;
    private final int numFailures;
    private final Properties consumerProps;
    private final Properties producerProps;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public IOManager(int minInterval, int averagingWindow, int numFailures, String brokerList) {

        this.replayCounter = new ReplayCounter();
        this.minInterval = minInterval;
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

        IOManager.executeUpdate("PRAGMA journal_mode=WAL;");
        IOManager.executeUpdate("PRAGMA synchronous=off;");

        LOG.info("Starting create events table");
        String createTable =
            "CREATE TABLE IF NOT EXISTS events " +
            "(timestamp INTEGER NOT NULL, " +
            "body TEXT NOT NULL);";
        IOManager.executeUpdate(createTable);
        LOG.info("Finished create events table");

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
            IOManager.executeUpdate(insertValue.append(";").toString());
        }
        LOG.info("Finished record events");

        LOG.info("Starting create index");
        String createIndex =
            "CREATE INDEX IF NOT EXISTS timestamp_index " +
            "ON events (timestamp);";
        IOManager.executeUpdate(createIndex);
        LOG.info("Finished create index");

        IOManager.executeUpdate("PRAGMA synchronous=normal;");
    }

    public void extractFullWorkload() {

        LOG.info("Starting create full workload table");
        String createTable =
            "CREATE TABLE IF NOT EXISTS workload " +
            "(second INTEGER NOT NULL, " +
            "timestamp INTEGER NOT NULL, " +
            "count INTEGER NOT NULL);";
        IOManager.executeUpdate(createTable);
        IOManager.executeUpdate("DELETE FROM workload;");
        LOG.info("Finished create full workload table");

        LOG.info("Starting extract full workload");
        String selectEvents =
            "SELECT timestamp, COUNT(*) AS count " +
            "FROM events GROUP BY timestamp";
        AtomicInteger second = new AtomicInteger(1);
        IOManager.executeQuery(selectEvents, (rs) -> {
            String insertValue = String.format(
                "INSERT INTO workload (second, timestamp, count) VALUES (%d,%d,%d);",
                second.getAndAdd(1), rs.getLong("timestamp"), rs.getInt("count"));
            IOManager.executeUpdate(insertValue);
        });
        LOG.info("Finished extract full workload");
    }

    public void updateEvents() {

        LOG.info("Starting update events");
        String selectEvents = "SELECT timestamp FROM events";
        IOManager.executeQuery(selectEvents, (rs) -> {
            long tsOld = rs.getLong("timestamp");
            long tsNew = (( (int) (tsOld / 1000)) + 1) * 1000;
            LOG.info(tsOld + " " + tsNew);
            String updateValue = String.format("UPDATE events SET timestamp = %d WHERE timestamp = %d;", tsNew, tsOld);
            IOManager.executeUpdate(updateValue);
        });
        LOG.info("Finished update events");
    }

    public List<Tuple3<Integer, Long, Integer>> getFullWorkload() {

        List<Tuple3<Integer, Long, Integer>> fullWorkload = new ArrayList<>();

        LOG.info("Starting get workload");
        String selectValues =
            "SELECT second, timestamp, count " +
            "FROM workload ORDER BY timestamp ASC";
        IOManager.executeQuery(selectValues, (rs) -> {
            fullWorkload.add(new Tuple3<>(rs.getInt("second"), rs.getLong("timestamp"), rs.getInt("count")));
        });
        LOG.info("Finished get workload");

        return fullWorkload;
    }

    public void extractFailureScenario(float tolerance) {

        List<Tuple3<Integer, Long, Integer>> fullWorkload = this.getFullWorkload();

        // find list of the max and minimum points
        List<Tuple3<Integer, Long, Integer>> min = new ArrayList<>();
        List<Tuple3<Integer, Long, Integer>> max = new ArrayList<>();
        for (int i = this.minInterval; i < fullWorkload.size() - this.minInterval; i++) {
            int sum = 0;
            for (int j = i - this.averagingWindow + 1; j <= i; j++) {
                sum += fullWorkload.get(j)._3();
            }
            int average = sum / this.averagingWindow;
            if (min.isEmpty()) min.add(fullWorkload.get(i));
            else if (min.get(0)._3() == average) min.add(fullWorkload.get(i));
            else if (average < min.get(0)._3()) {
                min.clear();
                min.add(fullWorkload.get(i));
            }
            if (max.isEmpty()) max.add(fullWorkload.get(i));
            else if (max.get(0)._3() == average) max.add(fullWorkload.get(i));
            else if (max.get(0)._3() < average) {
                max.clear();
                max.add(fullWorkload.get(i));
            }
        }
        // test if max and min were found
        if (min.size() == 0 || max.size() == 0) throw new IllegalStateException("Unable to find Max and/or Min intersects");
        // find values for average throughput where failures should be injected
        int maxVal = max.get(0)._3();
        int minVal = min.get(0)._3();
        // test for only 1 failure to be injected, min of 2 is required, i.e. max and min
        //int numFailures = (this.numFailures > 0) ? this.numFailures - 1 : 1;
        int step = (int) (((maxVal - minVal) * 1.0 / (this.numFailures -1)) + 0.5);
        // create map of for all points of intersection
        Map<Integer, List<Tuple3<Integer, Long, Integer>>> intersects = new TreeMap<>();
        intersects.put(minVal, min);
        Stream.iterate(minVal, i -> i + step).limit(this.numFailures - 1).forEach(i -> intersects.putIfAbsent(i, new ArrayList<>()));
        intersects.put(maxVal, max);
        // find all points of intersection in workload
        for (int i = this.minInterval; i < fullWorkload.size() - this.minInterval; i++) {

            int sum = 0;
            for (int j = i - this.averagingWindow + 1; j <= i; j++) {

                sum += fullWorkload.get(j)._3();
            }
            int average = sum / this.averagingWindow;
            // find range of values which is within user defined tolerance of average
            int onePercent = (int) ((fullWorkload.size() / 100) + 0.5);
            int withTolerance = (int) (onePercent * (tolerance / 2));
            List<Integer> targetRange =
                IntStream
                    .rangeClosed(average - withTolerance , average + withTolerance)
                    .boxed()
                    .collect(Collectors.toList());
            // find all intersects that are in 1% range of target
            for (int valueInRange : targetRange) {

                if (intersects.containsKey(valueInRange))

                    intersects.get(valueInRange).add(fullWorkload.get(i));
            }
        }
        // test if enough points were found at each intersect
        intersects.forEach((k,v) -> {
            LOG.info(k + " " + v.size());
            if (v.size() == 0) throw new IllegalStateException("Unable to find intersect for " + k);
        });
        // randomly select combination of intersects where at least one full set exists
        STOPWATCH.reset();
        STOPWATCH.start();
        while (true) {

            List<Tuple3<Integer, Long, Integer>> scenario = new ArrayList<>();
            intersects.forEach((k,v) -> scenario.add(v.get(RANDOM.nextInt(v.size()))));
            boolean valid = true;
            for (int i = 0; i < scenario.size(); i++) {

                for (int j = i+1; j < scenario.size(); j++) {

                    if (Math.abs(scenario.get(i)._1() - scenario.get(j)._1()) < this.minInterval) {

                        valid = false;
                        break;
                    }
                }
            }
            if (valid) {

                String createTable =
                    "CREATE TABLE IF NOT EXISTS scenario " +
                    "(second INTEGER NOT NULL, " +
                    "timestamp INTEGER NOT NULL, " +
                    "count INTEGER NOT NULL);";
                IOManager.executeUpdate(createTable);
                IOManager.executeUpdate("DELETE FROM scenario;");

                StringBuilder insertValue = new StringBuilder().append("INSERT INTO scenario (second, timestamp, count) VALUES ");
                Iterator<Tuple3<Integer, Long, Integer>> iterator = scenario.listIterator();
                while (iterator.hasNext()) {

                    Tuple3<Integer, Long, Integer> current = iterator.next();
                    insertValue.append(String.format("(%d,%d,%d)", current._1(), current._2(), current._3()));
                    if (iterator.hasNext()) insertValue.append(",");
                }
                IOManager.executeUpdate(insertValue.append(";").toString());
                STOPWATCH.stop();
                return;
            }
            if (STOPWATCH.getTime(TimeUnit.SECONDS) > 120)
                throw new IllegalStateException("Unable to find combination of intersects");
        }
    }

    public List<Tuple3<Integer, Long, Integer>> getFailureScenario() {

        List<Tuple3<Integer, Long, Integer>> scenario = new ArrayList<>();

        LOG.info("Starting get scenario");
        String selectValues =
            "SELECT second, timestamp, count " +
            "FROM scenario ORDER BY second ASC";
        IOManager.executeQuery(selectValues, (rs) -> {
            scenario.add(new Tuple3<>(rs.getInt("second"), rs.getLong("timestamp"), rs.getInt("count")));
        });
        LOG.info("Finished get scenario");
        return scenario;
    }

    public void initMetrics(int experimentId, boolean removePrevious) {

        // TODO remove!
        //IOManager.executeUpdate("DROP TABLE IF EXISTS metrics;");

        String createTable =
            "CREATE TABLE IF NOT EXISTS metrics " +
            "(experimentId INTEGER NOT NULL, " +
            "jobId TEXT NOT NULL, " +
            "timestamp INTEGER NOT NULL, " +
            "avgThr REAL NOT NULL, " +
            "avgLat REAL NOT NULL, " +
            "recTime REAL);";
        IOManager.executeUpdate(createTable);
        if (removePrevious) {

            IOManager.executeUpdate(String.format("DELETE FROM metrics WHERE experimentId = %d;", experimentId));
        }
    }

    public void addMetrics(int experimentId, String jobId, long timestamp, double avgThr, double avgLat) {

        String insertValue = String.format(
            "INSERT INTO metrics " +
            "(experimentId, jobId, timestamp, avgThr, avgLat) " +
            "VALUES " +
            "(%d, '%s', %d, %f, %f);",
            experimentId, jobId, timestamp, avgThr, avgLat);
        IOManager.executeUpdate(insertValue);
    }

    public List<Tuple6<Integer, String, Long, Double, Double, Double>> fetchMetrics(int experimentId, String jobId) {

        List<Tuple6<Integer, String, Long, Double, Double, Double>> metrics = new ArrayList<>();
        String selectValues = String.format(
            "SELECT experimentId, jobId, timestamp, avgThr, avgLat, recTime " +
            "FROM metrics " +
            "WHERE experimentId = %d " +
            "AND jobId = '%s' " +
            "ORDER BY timestamp ASC;",
            experimentId, jobId);
        IOManager.executeQuery(selectValues, (rs) -> {
            metrics.add(
                new Tuple6<>(
                    rs.getInt("experimentId"),
                    rs.getString("jobId"),
                    rs.getLong("timestamp"),
                    rs.getDouble("avgThr"),
                    rs.getDouble("avgLat"),
                    rs.getDouble("recTime")));
        });
        return metrics;
    }

    public void updateRecTime(String jobId, long timestamp, double recTime) {

        String updateValue = String.format(
            "UPDATE metrics " +
            "SET recTime = %f " +
            "WHERE jobId = '%s' " +
            "AND timestamp = %d;",
            recTime, jobId, timestamp);
        IOManager.executeUpdate(updateValue);
    }

    public void initJobs(int experimentId, boolean removePrevious) {

        // TODO remove
        //IOManager.executeUpdate("DROP TABLE IF EXISTS jobs;");
        String createTable =
            "CREATE TABLE IF NOT EXISTS jobs " +
            "(experimentId INTEGER NOT NULL, " +
            "jobId TEXT NOT NULL, " +
            "config REAL NOT NULL, " +
            "minDuration INTEGER NOT NULL, " +
            "avgDuration INTEGER NOT NULL, " +
            "maxDuration INTEGER NOT NULL, " +
            "minSize INTEGER NOT NULL, " +
            "avgSize INTEGER NOT NULL, " +
            "maxSize INTEGER NOT NULL, " +
            "startTs INTEGER NOT NULL, " +
            "stopTs INTEGER NOT NULL);";
        IOManager.executeUpdate(createTable);
        if (removePrevious) {

            IOManager.executeUpdate(String.format("DELETE FROM jobs WHERE experimentId = %d;", experimentId));
        }
    }

    public void addJob(
            int experimentId, String jobId, double config, long minDuration, long avgDuration,
            long maxDuration, long minSize, long avgSize, long maxSize, long startTs, long stopTs) {

        String insertValue = String.format(
            "INSERT INTO jobs " +
            "(experimentId, jobId, config, minDuration, avgDuration, maxDuration, minSize, avgSize, maxSize, startTs, stopTs) " +
            "VALUES " +
            "(%d, '%s', %f, %d, %d, %d, %d, %d, %d, %d, %d);",
            experimentId, jobId, config, minDuration, avgDuration, maxDuration, minSize, avgSize, maxSize, startTs, stopTs);
        IOManager.executeUpdate(insertValue);
    }

    public List<Tuple11<Integer, String, Double, Long, Long, Long, Long, Long, Long, Long, Long>> fetchJobs(int experimentId) {

        List<Tuple11<Integer, String, Double, Long, Long, Long, Long, Long, Long, Long, Long>> jobs = new ArrayList<>();
        String selectValues = String.format(
            "SELECT experimentId, jobId, config, minDuration, avgDuration, maxDuration, minSize, avgSize, maxSize, startTs, stopTs " +
            "FROM jobs " +
            "WHERE experimentId = %d " +
            "ORDER BY config ASC;",
            experimentId);
        IOManager.executeQuery(selectValues, (rs) -> {
            jobs.add(
                new Tuple11<>(
                    rs.getInt("experimentId"),
                    rs.getString("jobId"),
                    rs.getDouble("config"),
                    rs.getLong("minDuration"),
                    rs.getLong("avgDuration"),
                    rs.getLong("maxDuration"),
                    rs.getLong("minSize"),
                    rs.getLong("avgSize"),
                    rs.getLong("maxSize"),
                    rs.getLong("startTs"),
                    rs.getLong("stopTs")));
        });
        return jobs;
    }

    public void registerListener(Listener listener) {

        this.replayCounter.register(listener);
    }

    public DatabaseToQueue databaseToQueue(int startIndex, int stopIndex, BlockingQueue<List<String>> queue) {

        List<Tuple3<Integer, Long, Integer>> currWorkload = this.getFullWorkload().subList(startIndex, stopIndex);
        return new DatabaseToQueue(currWorkload, queue);
    }

    public QueueToKafka queueToKafka(int startIndex, BlockingQueue<List<String>> queue, String topic, AtomicBoolean isDone) {

        this.replayCounter.resetCounter(startIndex);
        return new QueueToKafka(queue, this.replayCounter, this.producerProps, topic, isDone);
    }
}
