package de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus;

import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.responses.KeyVal;
import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.responses.Matrix;
import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.responses.Vector;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.io.IOException;

public class PrometheusApiClient {

    public final String baseUrl;

    private final Retrofit retrofit;
    private final PrometheusRest service;

    public PrometheusApiClient(String baseUrl) {

        this.baseUrl = "http://" + baseUrl + "/";
        this.retrofit =
            new Retrofit.Builder()
                .baseUrl(this.baseUrl)
                .addConverterFactory(GsonConverterFactory.create())
                .build();
        this.service = retrofit.create(PrometheusRest.class);
    }

    public Vector query(String query) throws IOException {

        return this.service.query(query, null, null).execute().body();
    }

    public Vector query(String query, String time) throws IOException {

        return this.service.query(query, time, null).execute().body();
    }

    public Vector query(String query, String time, String timeout) throws IOException {

        return this.service.query(query, time, timeout).execute().body();
    }

    public Matrix queryRange(String query, String start, String end, String step, String timeout) throws IOException {

        return this.service.queryRange(query, start, end, step, timeout).execute().body();
    }

    public KeyVal findSeries(String match, String start, String end) throws IOException {

        return this.service.findSeries(match, start, end).execute().body();
    }
}
