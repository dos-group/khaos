package de.tu_berlin.dos.arm.khaos.clients.prometheus;

import de.tu_berlin.dos.arm.khaos.clients.prometheus.responses.Matrix;
import de.tu_berlin.dos.arm.khaos.events.Observation;
import de.tu_berlin.dos.arm.khaos.events.TimeSeries;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class PrometheusClient {

    private static final int LIMIT = 11000;

    private final PrometheusRest service;

    public PrometheusClient(String baseUrl) {

        Retrofit retrofit =
            new Retrofit.Builder()
                .baseUrl("http://" + baseUrl + "/")
                .addConverterFactory(GsonConverterFactory.create())
                .build();
        this.service = retrofit.create(PrometheusRest.class);
    }

    public TimeSeries queryRange(String query, long start, long end) throws IOException {

        TimeSeries ts = TimeSeries.create(start, end);
        long current = start;
        while (current < end) {

            Matrix matrix = this.service.queryRange(query, current, end, 1, 120).execute().body();
            List<List<String>> values = Objects.requireNonNull(matrix).data.result.get(0).values;
            for (List<String> strings : values) {

                long timestamp = Long.parseLong(strings.get(0));
                double value = Double.parseDouble(strings.get(1));
                ts.setObservation(new Observation(timestamp, value));
            }
            current += LIMIT;
        }
        return ts;
    }
}
