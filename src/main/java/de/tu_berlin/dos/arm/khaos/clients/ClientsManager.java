package de.tu_berlin.dos.arm.khaos.clients;

import de.tu_berlin.dos.arm.khaos.clients.flink.FlinkClient;
import de.tu_berlin.dos.arm.khaos.clients.flink.responses.Job;
import de.tu_berlin.dos.arm.khaos.clients.flink.responses.Vertices;
import de.tu_berlin.dos.arm.khaos.clients.flink.responses.Vertices.Node;
import de.tu_berlin.dos.arm.khaos.clients.kubernetes.KubernetesClient;
import de.tu_berlin.dos.arm.khaos.clients.prometheus.PrometheusClient;
import de.tu_berlin.dos.arm.khaos.io.TimeSeries;
import org.apache.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ClientsManager {

    /******************************************************************************
     * CLASS VARIABLES
     ******************************************************************************/

    private static final Logger LOG = Logger.getLogger(ClientsManager.class);
    private static final String LATENCY = "flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency";
    private static final String THROUGHPUT = "flink_taskmanager_job_task_operator_KafkaConsumer_records_consumed_rate";
    private static final String CONSUMER_LAG = "flink_taskmanager_job_task_operator_KafkaConsumer_records_lag_max";

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    public final KubernetesClient k8sClient;
    public final PrometheusClient prom;
    public final FlinkClient flink;
    public final String jarId;
    public final int parallelism;
    public final String k8sNamespace;
    public final int averagingWindow;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public ClientsManager(String promUrl, String flinkUrl, String jarId, int parallelism, String k8sNamespace, int averagingWindow) {

        this.k8sClient = new KubernetesClient();
        this.prom = new PrometheusClient(promUrl);
        this.flink = new FlinkClient(flinkUrl);
        this.jarId = jarId;
        this.parallelism = parallelism;
        this.k8sNamespace = k8sNamespace;
        this.averagingWindow = averagingWindow;
    }

    /******************************************************************************
     * INSTANCE BEHAVIOUR
     ******************************************************************************/

    public String startJob(String programArgs) throws Exception {

        return this.flink.startJob(this.jarId, programArgs, this.parallelism).jobId;
    }

    public List<String> getOperatorIds(String jobId) throws Exception {

        // store list of operator ids
        List<Node> vertices = this.flink.getVertices(jobId).plan.nodes;
        List<String> operatorIds = new ArrayList<>();
        for (Vertices.Node vertex: vertices) {

            operatorIds.add(vertex.id);
        }
        return operatorIds;
    }

    public String getSinkOperatorId(String jobId, String sinkRegex) throws Exception {

        List<Node> vertices = this.flink.getVertices(jobId).plan.nodes;
        for (Vertices.Node vertex: vertices) {

            if (vertex.description.startsWith(sinkRegex)) {

                return vertex.id;
            }
        }
        throw new IllegalStateException("Unable to find sink operator for job with ID " + jobId);
    }

    public long getLatestTs(String jobId) throws Exception {

        long now = this.flink.getLatestTs(jobId).now;
        return (long) Math.ceil(now / 1000.0);
    }

    public long getLastCheckpoint(String jobId) throws Exception {

        long lastCkp = this.flink.getCheckpoints(jobId).latest.completed.latestAckTimestamp;
        return (long) Math.ceil(lastCkp / 1000.0);
    }

    public void injectFailure(String jobId, String operatorId) throws Exception {

        String podName = this.flink.getTaskManagers(jobId, operatorId).taskManagers.get(0).taskManagerId;
        this.k8sClient.execCommandOnPod(podName, this.k8sNamespace, "sh", "-c", "kill 1");
    }

    public long getUptime(String jobId) throws Exception {

        return this.flink.getUptime(jobId);
    }

    public TimeSeries getLatency(String jobId, String sinkId, long startTs, long stopTs) throws Exception {

        String query =
            String.format(
                "sum(%s{job_id=\"%s\",quantile=\"0.99\",operator_id=\"%s\"})" +
                "/count(%s{job_id=\"%s\",quantile=\"0.99\",operator_id=\"%s\"})",
                LATENCY, jobId, sinkId, LATENCY, jobId, sinkId);
        return this.prom.queryRange(query, startTs, stopTs);
    }

    public TimeSeries getThroughput(String jobId, long startTs, long stopTs) throws Exception {

        String queryThr = String.format("sum(%s{job_id=\"%s\"})", THROUGHPUT, jobId);
        return this.prom.queryRange(queryThr, startTs, stopTs);
    }

    public TimeSeries getConsumerLag(String jobId, long startTs, long stopTs) throws Exception {

        String queryLag = String.format(
            "sum(%s{job_id=\"%s\"})/count(%s{job_id=\"%s\"})",
            CONSUMER_LAG, jobId, CONSUMER_LAG, jobId);
        return this.prom.queryRange(queryLag, startTs, stopTs);
    }
}
