package de.tu_berlin.dos.arm.khaos.workload_manager;

import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.FlinkApiClient;
import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.PrometheusApiClient;
import de.tu_berlin.dos.arm.khaos.common.utils.FileReader;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Properties;

public enum Context { get;

    public final String brokerList;
    public final String consumerTopic;
    public final String producerTopic;
    public final int timeLimit;
    public final String originalFilePath;
    public final String tempSortDir;
    public final String sortedFilePath;
    public final String tsLabel;
    public final int minFailureInterval;
    public final int averagingWindowSize;
    public final int numFailures;
    public final String k8sNamespace;
    public final String flinkJobManagerUrl;
    public final String flinkJarid;
    public final String flinkJobName;
    public final int flinkParallelism;
    public final String flinkSinkOperatorId;
    public final int metricsNumOfConfigs;
    public final String metricsThroughput;
    public final String metricsLatency;
    public final String metricsConsumerLag;
    public final String metricsCpuLoad;
    public final String prometheusUrl;
    public final int prometheusLimit;

    public final ReplayCounter replayCounter;
    public final FlinkApiClient flinkApiClient;
    public final PrometheusApiClient prometheusApiClient;

    //public final Map<String, >

    Context() {

        try {
            // get properties file
            Properties props = FileReader.GET.read("iot.properties", Properties.class);
            // load properties into context
            this.brokerList = props.getProperty("kafka.brokerList");
            this.consumerTopic = props.getProperty("kafka.topic");
            this.producerTopic = this.consumerTopic + "-" + RandomStringUtils.random(10, true, true);
            this.timeLimit = Integer.parseInt(props.getProperty("dataset.timeLimit"));
            this.originalFilePath = props.getProperty("dataset.originalFilePath");
            this.tempSortDir = props.getProperty("dataset.tempSortDir");
            this.sortedFilePath = props.getProperty("dataset.sortedFilePath");
            this.tsLabel = props.getProperty("dataset.tsLabel");
            this.minFailureInterval = Integer.parseInt(props.getProperty("analysis.minFailureInterval"));
            this.averagingWindowSize = Integer.parseInt(props.getProperty("analysis.averagingWindowSize"));
            this.numFailures = Integer.parseInt(props.getProperty("analysis.numFailures"));
            this.k8sNamespace = props.getProperty("k8s.namespace");
            this.flinkJobManagerUrl = props.getProperty("flink.jobManagerUrl");
            this.flinkJarid = props.getProperty("flink.jarid");
            this.flinkJobName = props.getProperty("flink.jobName");
            this.flinkParallelism = Integer.parseInt(props.getProperty("flink.parallelism"));
            this.flinkSinkOperatorId = props.getProperty("flink.sinkOperatorId");
            this.metricsNumOfConfigs = Integer.parseInt(props.getProperty("metrics.numOfConfigs"));
            this.metricsThroughput = props.getProperty("metrics.throughput");
            this.metricsLatency = props.getProperty("metrics.latency");
            this.metricsConsumerLag = props.getProperty("metrics.consumerLag");
            this.metricsCpuLoad = props.getProperty("metrics.cpuLoad");
            this.prometheusUrl = props.getProperty("prometheus.url");
            this.prometheusLimit = Integer.parseInt(props.getProperty("prometheus.limit"));
            // create global context objects
            this.replayCounter = new ReplayCounter();
            this.flinkApiClient = new FlinkApiClient(this.flinkJobManagerUrl);
            this.prometheusApiClient = new PrometheusApiClient(this.prometheusUrl);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException();
        }
    }
}
