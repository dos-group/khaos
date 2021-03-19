package de.tu_berlin.dos.arm.khaos.workload_manager;

import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.FlinkApiClient;
import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.PrometheusApiClient;
import de.tu_berlin.dos.arm.khaos.common.utils.FileReader;
import de.tu_berlin.dos.arm.khaos.workload_manager.modeling.RegressionModel;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

public enum Context { get;

    /******************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************/

    public static class Experiment {

        public static class FailureMetrics {

            public final long timestamp;
            public final double avgThr;
            public final double avgLat;
            public final double chkLast;

            private double recDur;

            public FailureMetrics(long timestamp, double avgThr, double avgLat, double chkLast) {
                this.timestamp = timestamp;
                this.avgThr = avgThr;
                this.avgLat = avgLat;
                this.chkLast = chkLast;
            }

            public double getRecDur() {

                return recDur;
            }

            public void setRecDur(double recDur) {

                this.recDur = recDur;
            }

            @Override
            public String toString() {
                return "FailureMetrics{" +
                        "timestamp=" + timestamp +
                        ", avgThr=" + avgThr +
                        ", avgLat=" + avgLat +
                        ", chkLast=" + chkLast +
                        ", recDur=" + recDur +
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
        public static long startTimestamp;
        public static long stopTimestamp;

        public final String jobName;
        public final double config;
        public final List<FailureMetrics> failureMetricsList;

        private String jobId;
        private List<String> operatorIds;
        private String sinkId;
        private CheckpointSummary chkSummary;

        public Experiment(String jobName, int config) {

            this.jobName = jobName;
            this.config = config;
            this.failureMetricsList = new ArrayList<>();
        }

        public String getJobId() {

            return jobId;
        }

        public void setJobId(String jobId) {

            this.jobId = jobId;
        }

        public void setOperatorIds(List<String> operatorIds) {

            this.operatorIds = operatorIds;
        }

        public List<String> getOperatorIds() {

            return operatorIds;
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

            return String.join(",",
                this.jobName,
                Experiment.brokerList,
                Experiment.consumerTopic,
                Experiment.producerTopic,
                Experiment.partitions + "",
                this.config + "");
        }

        @Override
        public String toString() {
            return "Experiment{" +
                    "startTimestamp=" + Experiment.startTimestamp +
                    ", stopTimestamp=" + Experiment.stopTimestamp +
                    ", jobName='" + jobName + '\'' +
                    ", config=" + config +
                    ", jobId='" + jobId + '\'' +
                    ", operatorIds=" + operatorIds +
                    ", sinkId='" + sinkId + '\'' +
                    ", chkSummary=" + chkSummary +
                    ", failureMetricsList=" + Arrays.toString(this.failureMetricsList.toArray()) +
                    '}';
        }

        // x_1 configs:     1000 1000 1000 1000 1000 14222 14222 14222 14222 14222
        // x_2 throughputs: 486 24148 32276 12676 18647 486 24148 32276 12676 18647

        // y latencies:   398 345 349 397 371 427 346 408 372 371 363

        // y recovery:  time*dist/ci
    }

    /******************************************************************************
     * CLASS VARIABLES
     ******************************************************************************/

    private static final Logger LOG = Logger.getLogger(Context.class);

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    public final List<Experiment> experiments;

    public final String brokerList;
    public final String consumerTopic;
    public final String producerTopic;
    public final int partitions;
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
    public final String jobManagerUrl;
    public final String jarId;
    public final String jobName;
    public final int parallelism;
    public final String sinkOperatorName;
    public final int numOfConfigs;
    public final int minConfigVal;
    public final int maxConfigVal;
    public final String prometheusUrl;
    public final String throughput;
    public final String latency;
    public final String consumerLag;
    public final String topicMsgPerSec;

    public final ReplayCounter replayCounter;
    public final FlinkApiClient flinkApiClient;
    public final FailureInjector failureInjector;
    public final PrometheusApiClient prometheusApiClient;

    public WorkloadAnalyser analyzer;
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
            this.brokerList = props.getProperty("kafka.brokerList");
            this.consumerTopic = props.getProperty("kafka.consumerTopic");
            this.producerTopic = props.getProperty("kafka.producerTopic");
            this.partitions = Integer.parseInt(props.getProperty("kafka.partitions"));
            this.timeLimit = Integer.parseInt(props.getProperty("dataset.timeLimit"));
            this.originalFilePath = props.getProperty("dataset.originalFilePath");
            this.tempSortDir = props.getProperty("dataset.tempSortDir");
            this.sortedFilePath = props.getProperty("dataset.sortedFilePath");
            this.tsLabel = props.getProperty("dataset.tsLabel");
            this.minFailureInterval = Integer.parseInt(props.getProperty("analysis.minFailureInterval"));
            this.averagingWindow = Integer.parseInt(props.getProperty("analysis.averagingWindow"));
            this.numFailures = Integer.parseInt(props.getProperty("analysis.numFailures"));
            this.k8sNamespace = props.getProperty("k8s.namespace");
            this.backupFolder = props.getProperty("flink.backupFolder");
            this.jobManagerUrl = props.getProperty("flink.jobManagerUrl");
            this.jarId = props.getProperty("flink.jarid");
            this.jobName = props.getProperty("flink.jobName");
            this.parallelism = Integer.parseInt(props.getProperty("flink.parallelism"));
            this.sinkOperatorName = props.getProperty("flink.sinkOperatorName");
            this.numOfConfigs = Integer.parseInt(props.getProperty("experiments.numOfConfigs"));
            this.minConfigVal = Integer.parseInt(props.getProperty("experiments.minConfigVal"));
            this.maxConfigVal = Integer.parseInt(props.getProperty("experiments.maxConfigVal"));
            this.prometheusUrl = props.getProperty("prometheus.url");
            this.throughput = props.getProperty("metrics.throughput");
            this.latency = props.getProperty("metrics.latency");
            this.consumerLag = props.getProperty("metrics.consumerLag");
            this.topicMsgPerSec = props.getProperty("metrics.topicMsgPerSec");

            // create global context objects
            this.replayCounter = new ReplayCounter();
            this.flinkApiClient = new FlinkApiClient(this.jobManagerUrl);
            this.failureInjector = new FailureInjector();
            this.prometheusApiClient = new PrometheusApiClient(this.prometheusUrl, 1, 120);

            // create multiple regression models
            this.performance = new RegressionModel();
            this.availability = new RegressionModel();

            // set global experiment variables
            Experiment.brokerList = this.brokerList;
            Experiment.consumerTopic = this.consumerTopic + "-" + RandomStringUtils.random(10, true, true);
            Experiment.producerTopic = this.producerTopic + "-" + RandomStringUtils.random(10, true, true);
            Experiment.partitions = this.partitions;

            // instantiate experiments list
            this.experiments = new ArrayList<>();
            int step = (int) (((this.maxConfigVal - this.minConfigVal) * 1.0 / (this.numOfConfigs - 1)) + 0.5);
            Stream.iterate(this.minConfigVal, i -> i + step).limit(this.numOfConfigs).forEach(config -> {
                String uniqueJobName = this.jobName + "-" + RandomStringUtils.random(10, true, true);
                this.experiments.add(new Experiment(uniqueJobName, config));
            });
        }
        catch (Exception e) {

            throw new IllegalStateException(e.getMessage());
        }
    }
}
