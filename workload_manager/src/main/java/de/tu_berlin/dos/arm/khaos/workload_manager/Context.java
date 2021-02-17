package de.tu_berlin.dos.arm.khaos.workload_manager;

import de.tu_berlin.dos.arm.khaos.common.prometheus.PrometheusApiClient;
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
    public final int minTimeBetweenFailures;
    public final int numOfFailures;
    public final String k8sNamespace;
    public final String flinkJobName;
    public final String flinkSinkOperatorId;
    public final String metricsThroughput;
    public final String metricsLatency;
    public final String metricsConsumerLag;
    public final String metricscpuLoad;
    public final String prometheusUrl;
    public final int prometheusLimit;

    public final ReplayCounter replayCounter;
    public final PrometheusApiClient prometheusClient;

    Context() {

        try {
            // get properties file
            Properties props = FileReader.GET.read("workload.properties", Properties.class);
            // load properties into context
            this.brokerList = props.getProperty("kafka.brokerList");
            this.consumerTopic = props.getProperty("kafka.topic");
            this.producerTopic = this.consumerTopic + "-" + RandomStringUtils.random(10, true, true);
            this.timeLimit = Integer.parseInt(props.getProperty("dataset.timeLimit"));
            this.originalFilePath = props.getProperty("dataset.originalFilePath");
            this.tempSortDir = props.getProperty("dataset.tempSortDir");
            this.sortedFilePath = props.getProperty("dataset.sortedFilePath");
            this.tsLabel = props.getProperty("dataset.tsLabel");
            this.minTimeBetweenFailures = Integer.parseInt(props.getProperty("analysis.minTimeBetweenFailures"));
            this.numOfFailures = Integer.parseInt(props.getProperty("analysis.numOfFailures"));
            this.k8sNamespace = props.getProperty("k8s.namespace");
            this.flinkJobName = props.getProperty("flink.jobName");
            this.flinkSinkOperatorId = props.getProperty("flink.sinkOperatorId");
            this.metricsThroughput = props.getProperty("metrics.throughput");
            this.metricsLatency = props.getProperty("metrics.latency");
            this.metricsConsumerLag = props.getProperty("metrics.consumerLag");
            this.metricscpuLoad = props.getProperty("metrics.cpuLoad");
            this.prometheusUrl = props.getProperty("prometheus.url");
            this.prometheusLimit = Integer.parseInt(props.getProperty("prometheus.limit"));
            // create global context objects
            this.replayCounter = new ReplayCounter();
            this.prometheusClient = new PrometheusApiClient(this.prometheusUrl);
        }
        catch (Exception e) {

            throw new IllegalStateException();
        }
    }
}
