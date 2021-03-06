package de.tu_berlin.dos.arm.khaos.common.api_clients;

import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.FlinkRest;
import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.Checkpoints;
import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.PrometheusApiClient;
import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.PrometheusRest;
import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.responses.Matrix;
import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.responses.Vector;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.time.Instant;
import java.time.LocalTime;
import java.util.concurrent.CountDownLatch;

public class Run2  {

    public static void main(String[] args) throws Exception {

        String baseUrl = "http://130.149.249.40:30067/";
        Retrofit retrofit =
            new Retrofit.Builder()
                .baseUrl(baseUrl)
                .addConverterFactory(GsonConverterFactory.create())
                .build();
        FlinkRest flinkService = retrofit.create(FlinkRest.class);

        baseUrl = "http://130.149.249.40:31047/";
        retrofit =
            new Retrofit.Builder()
                .baseUrl(baseUrl)
                .addConverterFactory(GsonConverterFactory.create())
                .build();
        PrometheusRest prometheusService = retrofit.create(PrometheusRest.class);

        String jobId = "0c67ebeeb08197566a1967f1e480b516";

        Checkpoints checkpoints = flinkService.getCheckpoints(jobId).execute().body();

        // last checkpoint complete status
        System.out.println(checkpoints.latest.completed.status);

        // last checkpoint timestamp
        System.out.println(checkpoints.latest.completed.latestAckTimestamp);

        Instant instant = Instant.now();
        System.out.println(instant.toEpochMilli() - checkpoints.latest.completed.latestAckTimestamp);

        String latency = "flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency";
        String operatorId = "46f8730428df9ecd6d7318a02bdc405e";



        String query = String.format("sum(%s{job_name=\"vehicles\",quantile=\"0.95\",operator_id=\"%s\"})/count(%s{job_name=\"vehicles\",quantile=\"0.95\",operator_id=\"%s\"})", latency, operatorId, latency, operatorId);
        System.out.println(query);
        Vector vector = prometheusService.query(query, String.valueOf(Instant.now().toEpochMilli()), "120000").execute().body();
        System.out.println(vector.data.result);
        instant = Instant.now();
        Matrix matrix = prometheusService.queryRange(query, instant.getEpochSecond() - 300 + "", instant.getEpochSecond() + "", "1","120").execute().body();
        System.out.println(matrix.data.result);

        double sum = 0;
        int count = 0;
        for (int i = 0; i < matrix.data.result.get(0).values.size(); i++) {

            sum += matrix.data.result.get(0).values.get(i).get(1);
            count++;
        }
        System.out.println(sum / count);
    }
}
