package de.tu_berlin.dos.arm.khaos.core;

import de.tu_berlin.dos.arm.khaos.clients.ClientsManager;
import de.tu_berlin.dos.arm.khaos.io.IOManager;
import de.tu_berlin.dos.arm.khaos.modeling.RegressionModel;
import de.tu_berlin.dos.arm.khaos.utils.FileReader;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.stream.Stream;

public enum Context { get;

    /******************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************/

    public static class StreamingJob {

        public static String brokerList;
        public static String consumerTopic;
        public static String producerTopic;
        public static int partitions;
        public static long startTs;
        public static long stopTs;

        public final String jobName;

        private String jobId;
        private int config;
        private List<String> operatorIds;
        private String sinkId;

        public StreamingJob(String jobName) {

            this.jobName = jobName;
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

        public void setConfig(int config) {

            this.config = config;
        }

        public void setOperatorIds(List<String> operatorIds) {

            this.operatorIds = operatorIds;
        }

        public void setSinkId(String sinkId) {

            this.sinkId = sinkId;
        }

        public String getSinkId() {

            return sinkId;
        }

        public String getProgramArgs() {

            return String.join(",", this.jobName, brokerList, consumerTopic, producerTopic, partitions + "", (int) this.config + "");
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

    public final IOManager IOManager;
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
            this.IOManager = new IOManager(this.minFailureInterval, this.averagingWindow, this.numFailures, this.brokerList);
            this.clientsManager = new ClientsManager(this.promUrl, this.flinkUrl, this.jarId, this.parallelism, this.k8sNamespace, this.averagingWindow);

            // create multiple regression models
            this.performance = new RegressionModel();
            this.availability = new RegressionModel();
        }
        catch (Exception e) {

            throw new IllegalStateException(e.fillInStackTrace());
        }
    }
}
