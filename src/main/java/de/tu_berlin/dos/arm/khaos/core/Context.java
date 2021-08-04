package de.tu_berlin.dos.arm.khaos.core;

import de.tu_berlin.dos.arm.khaos.clients.ClientsManager;
import de.tu_berlin.dos.arm.khaos.io.IOManager;
import de.tu_berlin.dos.arm.khaos.modeling.ForecastModel;
import de.tu_berlin.dos.arm.khaos.modeling.RegressionModel;
import de.tu_berlin.dos.arm.khaos.modeling.RegressionModelOld;
import de.tu_berlin.dos.arm.khaos.utils.FileReader;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

public enum Context implements AutoCloseable { get;

    /******************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************/

    public static class Job {

        public final Experiment experiment;
        public final String jobName;

        private String jobId;
        private int config;
        private List<String> operatorIds;
        private String sinkId;

        public Job(Experiment experiment, String jobName) {

            this.experiment = experiment;
            this.jobName = jobName;
        }

        public String getJobId() {

            return jobId;
        }

        public void setJobId(String jobId) {

            this.jobId = jobId;
        }

        public int getConfig() {

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

            return String.join(",",
                this.jobName,
                experiment.brokerList,
                experiment.consumerTopic,
                experiment.producerTopic,
                experiment.partitions + "",
                this.config + "");
        }

        @Override
        public String toString() {

            return "Job{" +
                    "jobName='" + jobName + '\'' +
                    ", config=" + config +
                    ", jobId='" + jobId + '\'' +
                    ", operatorIds=" + operatorIds +
                    ", sinkId='" + sinkId + '\'' +
                    '}';
        }
    }

    public static class Experiment {

        public final int experimentId;
        public final String brokerList;
        public final String consumerTopic;
        public final String producerTopic;
        public final int partitions;
        public final List<Job> jobs;

        private long startTs;
        private long stopTs;

        public Experiment(
                int experimentId, String brokerList, String consumerTopic, String producerTopic,
                int partitions, int minConfigVal, int maxConfigVal, int numOfConfigs) {

            String uniqueStr = RandomStringUtils.random(10, true, true);
            this.experimentId = experimentId;
            this.brokerList = brokerList;
            //this.consumerTopic = consumerTopic + "-" + uniqueStr;
            //this.producerTopic = producerTopic + "-" + uniqueStr;
            this.consumerTopic = consumerTopic;
            this.producerTopic = producerTopic;
            this.partitions = partitions;

            this.jobs = new ArrayList<>();
            /*int step = (int) (((maxConfigVal - minConfigVal) * 1.0 / (numOfConfigs - 1)) + 0.5);
            Stream.iterate(minConfigVal, i -> i + step).limit(numOfConfigs).forEach(config -> {
                Job current = new Job(this, "profile-" + config, config);
                this.jobs.add(current);
            });*/
            // TODO remove!
            //for (int config : Arrays.asList(10000, 30000, 60000, 90000, 120000)) {
            for (int config : Arrays.asList(10000, 30000)) {

                Job job = new Job(this, "iot_profile_" + config);
                job.setConfig(config);
                this.jobs.add(job);
            }
        }

        public long getStartTs() {

            return startTs;
        }

        public void setStartTs(long startTs) {

            this.startTs = startTs;
        }

        public long getStopTs() {

            return stopTs;
        }

        public void setStopTs(long stopTs) {

            this.stopTs = stopTs;
        }
    }

    public enum ExecutorType {

        NEW_SINGLE_THREAD_SCHEDULED_EXECUTOR;
    }

    /******************************************************************************
     * CLASS VARIABLES
     ******************************************************************************/

    private static final Logger LOG = Logger.getLogger(Context.class);

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    public final long avgLatConst;
    public final long recTimeConst;
    public final long optInterval;
    public final int minUpTime;
    public final int averagingWindow;
    public final float maxViolation;
    public final int experimentId;
    public final int numOfConfigs;
    public final int minConfigVal;
    public final int maxConfigVal;
    public final int minInterval;
    public final int numFailures;
    public final int chkTolerance;
    public final boolean doRecord;
    public final int timeLimit;
    public final String k8sNamespace;
    public final String brokerList;
    public final String consumerTopic;
    public final String producerTopic;
    public final int partitions;
    public final String backupFolder;
    public final String flinkUrl;
    public final String jarId;
    public final String jobName;
    public final String jobId;
    public final int jobConfig;
    public final int parallelism;
    public final String sinkRegex;
    public final String savepoints;
    public final String promUrl;

    public final Experiment experiment;
    public final Job targetJob;

    public final Map<ExecutorType, ExecutorService> executors = new HashMap<>();
    public final ScheduledExecutorService executor;
    public final IOManager IOManager;
    public final ClientsManager clientsManager;

    public ForecastModel forecast;

    public RegressionModel performance;
    public RegressionModel availability;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    Context() {

        try {

            // get properties file
            Properties props = FileReader.GET.read("advertising.properties", Properties.class);

            // load properties into context
            this.avgLatConst = Long.parseLong(props.getProperty("general.avgLatConst"));
            this.recTimeConst = Long.parseLong(props.getProperty("general.recTimeConst"));
            this.optInterval = Long.parseLong(props.getProperty("general.optInterval"));
            this.minUpTime = Integer.parseInt(props.getProperty("general.minUpTime"));
            this.averagingWindow = Integer.parseInt(props.getProperty("general.averagingWindow"));
            this.maxViolation = Float.parseFloat(props.getProperty("general.maxViolation"));
            this.experimentId = Integer.parseInt(props.getProperty("experiment.id"));
            this.numOfConfigs = Integer.parseInt(props.getProperty("experiment.numOfConfigs"));
            this.minConfigVal = Integer.parseInt(props.getProperty("experiment.minConfigVal"));
            this.maxConfigVal = Integer.parseInt(props.getProperty("experiment.maxConfigVal"));
            this.minInterval = Integer.parseInt(props.getProperty("experiment.minInterval"));
            this.numFailures = Integer.parseInt(props.getProperty("experiment.numFailures"));
            this.chkTolerance = Integer.parseInt(props.getProperty("experiment.chkTolerance"));
            this.doRecord = Boolean.parseBoolean(props.getProperty("database.doRecord"));
            this.timeLimit = Integer.parseInt(props.getProperty("database.timeLimit"));
            this.k8sNamespace = props.getProperty("k8s.namespace");
            this.brokerList = props.getProperty("kafka.brokerList");
            this.consumerTopic = props.getProperty("kafka.consumerTopic");
            this.producerTopic = props.getProperty("kafka.producerTopic");
            this.partitions = Integer.parseInt(props.getProperty("kafka.partitions"));
            this.backupFolder = props.getProperty("flink.backupFolder");
            this.flinkUrl = props.getProperty("flink.jobManagerUrl");
            this.jarId = props.getProperty("flink.jarid");
            this.jobName = props.getProperty("flink.jobName");
            this.jobId = props.getProperty("flink.jobId");
            this.jobConfig = Integer.parseInt(props.getProperty("flink.jobConfig"));
            this.parallelism = Integer.parseInt(props.getProperty("flink.parallelism"));
            this.sinkRegex = props.getProperty("flink.sinkRegex");
            this.savepoints = props.getProperty("flink.savepoints");
            this.promUrl = props.getProperty("prometheus.url");

            // set experiment variables
            String uniqueString = RandomStringUtils.random(10, true, true);
            this.experiment = new Experiment(
                this.experimentId,
                this.brokerList,
                this.consumerTopic,
                this.producerTopic,
                this.partitions,
                this.minConfigVal,
                this.maxConfigVal,
                this.numOfConfigs
            );

            // create object for target job and store list of operator ids
            this.targetJob = new Job(experiment, this.jobName);
            this.targetJob.setJobId(this.jobId);
            this.targetJob.setConfig(this.jobConfig);

            // create global context objects
            this.executors.put(ExecutorType.NEW_SINGLE_THREAD_SCHEDULED_EXECUTOR, Executors.newSingleThreadScheduledExecutor());
            this.executor = Executors.newSingleThreadScheduledExecutor();
            this.IOManager = new IOManager(this.minInterval, this.averagingWindow, this.numFailures, this.brokerList);
            this.clientsManager = new ClientsManager(this.promUrl, this.flinkUrl, this.jarId, this.parallelism, this.k8sNamespace, this.averagingWindow);

            // create forecast model
            this.forecast = new ForecastModel(
                Integer.parseInt(props.getProperty("arima.coefficient.p")),
                Integer.parseInt(props.getProperty("arima.coefficient.d")),
                Integer.parseInt(props.getProperty("arima.coefficient.q")),
                Boolean.parseBoolean(props.getProperty("arima.coefficient.constant")),
                props.getProperty("arima.strategy")
            );

            // create multiple regression models
            this.performance = new RegressionModel();
            this.availability = new RegressionModel();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException(e.fillInStackTrace());
        }
    }

    @Override
    public void close() throws Exception {

        for (ExecutorService executor : this.executors.values()) {

            executor.shutdown();
        }
        this.executor.shutdown();
    }
}
