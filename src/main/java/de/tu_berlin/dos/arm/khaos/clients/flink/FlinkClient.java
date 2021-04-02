package de.tu_berlin.dos.arm.khaos.clients.flink;

import de.tu_berlin.dos.arm.khaos.clients.flink.responses.*;
import okhttp3.OkHttpClient;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class FlinkClient {

    public final String baseUrl;
    public final FlinkRest service;

    public FlinkClient(String baseUrl) {

        this.baseUrl = "http://" + baseUrl + "/";
        OkHttpClient okHttpClient = new OkHttpClient.Builder()
            .connectTimeout(1, TimeUnit.MINUTES)
            .readTimeout(30, TimeUnit.SECONDS)
            .writeTimeout(15, TimeUnit.SECONDS)
            .build();
        Retrofit retrofit =
            new Retrofit.Builder()
                .baseUrl(this.baseUrl)
                .client(okHttpClient)
                .addConverterFactory(GsonConverterFactory.create())
                .build();

        this.service = retrofit.create(FlinkRest.class);
    }

    public Job startJob(String jarId, String programArg, int parallelism) throws IOException {

        return this.service.startJob(jarId, programArg, parallelism).execute().body();
    }

    public boolean stopJob(String jarId) throws IOException {

        return this.service.stopJob(jarId).execute().isSuccessful();
    }

    public TaskManagers getTaskManagers(String jobId, String vertexId) throws IOException {

        return this.service.getTaskManagers(jobId, vertexId).execute().body();
    }

    public Vertices getVertices(String jobId) throws IOException {

        return this.service.getVertices(jobId).execute().body();
    }

    public Checkpoints getCheckpoints(String jobId) throws IOException {

        return this.service.getCheckpoints(jobId).execute().body();
    }

    public long getUptime(String jobId) throws IOException {

        long timestamp = Objects.requireNonNull(this.service.getUptime(jobId).execute().body()).timestamps.lastRestart;
        return (System.currentTimeMillis() - timestamp) / 1000;
    }
}
