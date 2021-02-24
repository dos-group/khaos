package de.tu_berlin.dos.arm.khaos.common.api_clients.flink;

import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.StartJob;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.io.IOException;

public class FlinkApiClient {

    public final String baseUrl;
    public final FlinkRest service;

    public FlinkApiClient(String baseUrl) {

        this.baseUrl = baseUrl;
        Retrofit retrofit =
            new Retrofit.Builder()
                .baseUrl(baseUrl)
                .addConverterFactory(GsonConverterFactory.create())
                .build();
        this.service = retrofit.create(FlinkRest.class);
    }

    public StartJob startJob(String id, String programArg, int parallelism) throws IOException {

        return this.service.startJob(id, programArg, parallelism).execute().body();
    }

    public boolean stopJob(String id) throws IOException {

        return this.service.stopJob(id).execute().isSuccessful();
    }
}
