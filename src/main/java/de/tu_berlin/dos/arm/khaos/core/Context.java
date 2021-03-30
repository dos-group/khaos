package de.tu_berlin.dos.arm.khaos.core;

import de.tu_berlin.dos.arm.khaos.clients.ClientsManager;
import de.tu_berlin.dos.arm.khaos.events.EventsManager;
import de.tu_berlin.dos.arm.khaos.modeling.RegressionModel;
import de.tu_berlin.dos.arm.khaos.utils.FileReader;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Logger;
import smile.data.DataFrame;
import smile.data.formula.Formula;
import smile.regression.LinearModel;
import smile.regression.OLS;

import java.util.*;
import java.util.stream.Stream;

public enum Context { get;

    /******************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************/

    public static class StreamingJob {

        public static class FailureMetrics {

            public final long timestamp;
            public final double avgThr;
            public final double avgLat;
            public final double chkLast;

            private double duration;

            public FailureMetrics(long timestamp, double avgThr, double avgLat, double chkLast) {

                this.timestamp = timestamp;
                this.avgThr = avgThr;
                this.avgLat = avgLat;
                this.chkLast = chkLast;
            }

            public double getDuration() {

                return duration;
            }

            public void setDuration(double duration) {

                this.duration = duration;
            }

            @Override
            public String toString() {

                return "FailureMetrics{" +
                        "timestamp=" + timestamp +
                        ", avgThr=" + avgThr +
                        ", avgLat=" + avgLat +
                        ", chkLast=" + chkLast +
                        ", duration=" + duration +
                        '}';
            }
        }

        public static class CheckpointSummary {

            public final long minDuration;
            public final long avgDuration;
            public final long maxDuration;
            public final long minSize;
            public final long avgSize;
            public final long maxSize;

            public CheckpointSummary(
                    long minDuration, long avgDuration, long maxDuration,
                    long minSize, long avgSize, long maxSize) {

                this.minDuration = minDuration;
                this.avgDuration = avgDuration;
                this.maxDuration = maxDuration;
                this.minSize = minSize;
                this.avgSize = avgSize;
                this.maxSize = maxSize;
            }

            @Override
            public String toString() {

                return "CheckpointSummary{" +
                        "minDuration=" + minDuration +
                        ", avgDuration=" + avgDuration +
                        ", maxDuration=" + maxDuration +
                        ", minSize=" + minSize +
                        ", avgSize=" + avgSize +
                        ", maxSize=" + maxSize +
                        '}';
            }
        }

        public static String brokerList;
        public static String consumerTopic;
        public static String producerTopic;
        public static int partitions;
        public static long startTs;
        public static long stopTs;

        public final String jobName;
        public final List<FailureMetrics> failureMetricsList;

        private final Random rand = new Random();
        private String jobId;
        private double config;
        private List<String> operatorIds;
        private String sinkId;
        private CheckpointSummary chkSummary;

        public StreamingJob(String jobName) {

            this.jobName = jobName;
            this.failureMetricsList = new ArrayList<>();
        }

        public String getJobId() {

            return jobId;
        }

        public void setJobId(String jobId) {

            this.jobId = jobId;
        }

        public double getConfig() {

            return config;
        }

        public void setConfig(double config) {

            this.config = config;
        }

        public void setOperatorIds(List<String> operatorIds) {

            this.operatorIds = operatorIds;
        }

        public String getRandomOperatorId() {

            return operatorIds.get(rand.nextInt(operatorIds.size()));
        }

        public void setSinkId(String sinkId) {

            this.sinkId = sinkId;
        }

        public String getSinkId() {

            return sinkId;
        }

        public CheckpointSummary getChkSummary() {

            return chkSummary;
        }

        public void setChkSummary(CheckpointSummary chkSummary) {

            this.chkSummary = chkSummary;
        }

        public String getProgramArgs() {

            return String.join(",", this.jobName, brokerList, consumerTopic, producerTopic, partitions + "", this.config + "");
        }

        @Override
        public String toString() {

            return "Experiment{" +
                    "startTs=" + StreamingJob.startTs +
                    ", stopTs=" + StreamingJob.stopTs +
                    ", jobName='" + jobName + '\'' +
                    ", config=" + config +
                    ", jobId='" + jobId + '\'' +
                    ", operatorIds=" + operatorIds +
                    ", sinkId='" + sinkId + '\'' +
                    ", chkSummary=" + chkSummary +
                    ", failureMetricsList=" + Arrays.toString(this.failureMetricsList.toArray()) +
                    '}';
        }
    }

    /******************************************************************************
     * CLASS VARIABLES
     ******************************************************************************/

    private static final Logger LOG = Logger.getLogger(Context.class);

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    public final long constraintInterval;
    public final long constraintPerformance;
    public final long constraintAvailability;
    public final long constraintWarmupTime;
    public final String brokerList;
    public final String consumerTopic;
    public final String producerTopic;
    public final int partitions;
    public final String dbFileName;
    public final int timeLimit;
    public final String originalFilePath;
    public final String tempSortDir;
    public final String sortedFilePath;
    public final String tsLabel;
    public final int minFailureInterval;
    public final int averagingWindow;
    public final int numFailures;
    public final String k8sNamespace;
    public final String backupFolder;
    public final String flinkUrl;
    public final String jarId;
    public final String jobName;
    public final String jobId;
    public final int parallelism;
    public final String sinkRegex;
    public final int numOfConfigs;
    public final int minConfigVal;
    public final int maxConfigVal;
    public final String promUrl;
    public final String topicMsgPerSec;

    public final StreamingJob targetJob;
    public final List<StreamingJob> experiments;

    public final EventsManager eventsManager;
    public final ClientsManager clientsManager;

    public RegressionModel performance;
    public RegressionModel availability;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    Context() {

        try {

            // get properties file
            Properties props = FileReader.GET.read("iot.properties", Properties.class);

            // load properties into context
            this.constraintPerformance = Long.parseLong(props.getProperty("constraint.performance"));
            this.constraintAvailability = Long.parseLong(props.getProperty("constraint.availability"));
            this.constraintInterval = Long.parseLong(props.getProperty("constraint.interval"));
            this.constraintWarmupTime = Long.parseLong(props.getProperty("constraint.warmupTime"));
            this.brokerList = props.getProperty("kafka.brokerList");
            this.consumerTopic = props.getProperty("kafka.consumerTopic");
            this.producerTopic = props.getProperty("kafka.producerTopic");
            this.partitions = Integer.parseInt(props.getProperty("kafka.partitions"));
            this.dbFileName = props.getProperty("database.fileName");
            this.timeLimit = Integer.parseInt(props.getProperty("database.timeLimit"));
            this.originalFilePath = props.getProperty("dataset.originalFilePath");
            this.tempSortDir = props.getProperty("dataset.tempSortDir");
            this.sortedFilePath = props.getProperty("dataset.sortedFilePath");
            this.tsLabel = props.getProperty("dataset.tsLabel");
            this.minFailureInterval = Integer.parseInt(props.getProperty("analysis.minFailureInterval"));
            this.averagingWindow = Integer.parseInt(props.getProperty("analysis.averagingWindow"));
            this.numFailures = Integer.parseInt(props.getProperty("analysis.numFailures"));
            this.k8sNamespace = props.getProperty("k8s.namespace");
            this.backupFolder = props.getProperty("flink.backupFolder");
            this.flinkUrl = props.getProperty("flink.jobManagerUrl");
            this.jarId = props.getProperty("flink.jarid");
            this.jobName = props.getProperty("flink.jobName");
            this.jobId = props.getProperty("flink.jobId");
            this.parallelism = Integer.parseInt(props.getProperty("flink.parallelism"));
            this.sinkRegex = props.getProperty("flink.sinkRegex");
            this.numOfConfigs = Integer.parseInt(props.getProperty("experiments.numOfConfigs"));
            this.minConfigVal = Integer.parseInt(props.getProperty("experiments.minConfigVal"));
            this.maxConfigVal = Integer.parseInt(props.getProperty("experiments.maxConfigVal"));
            this.promUrl = props.getProperty("prometheus.url");
            this.topicMsgPerSec = props.getProperty("metrics.topicMsgPerSec");

            // set global experiment variables
            StreamingJob.brokerList = this.brokerList;
            StreamingJob.consumerTopic = this.consumerTopic + "-" + RandomStringUtils.random(10, true, true);
            StreamingJob.producerTopic = this.producerTopic + "-" + RandomStringUtils.random(10, true, true);
            StreamingJob.partitions = this.partitions;

            // create object for target job and store list of operator ids
            this.targetJob = new StreamingJob(this.jobName);
            this.targetJob.setJobId(this.jobId);

            // instantiate experiments list
            this.experiments = new ArrayList<>();
            int step = (int) (((this.maxConfigVal - this.minConfigVal) * 1.0 / (this.numOfConfigs - 1)) + 0.5);
            Stream.iterate(this.minConfigVal, i -> i + step).limit(this.numOfConfigs).forEach(config -> {
                String uniqueJobName = this.targetJob.jobName + "-" + RandomStringUtils.random(10, true, true);
                StreamingJob current = new StreamingJob(uniqueJobName);
                current.setConfig(config);
                this.experiments.add(current);
            });

            // create global context objects
            this.eventsManager = new EventsManager(this.minFailureInterval, this.averagingWindow, this.numFailures, this.brokerList);
            this.clientsManager = new ClientsManager(this.promUrl, this.flinkUrl, this.jarId, this.parallelism, this.k8sNamespace, this.averagingWindow);

            // create multiple regression models
            this.performance = new RegressionModel();
            this.availability = new RegressionModel();
        }
        catch (Exception e) {

            System.out.println(e.fillInStackTrace());
            throw new IllegalStateException(e.fillInStackTrace());
        }
    }
}
