package de.tu_berlin.dos.arm.khaos.clients.prometheus;

import de.tu_berlin.dos.arm.khaos.clients.prometheus.responses.Matrix;
import de.tu_berlin.dos.arm.khaos.io.Observation;
import de.tu_berlin.dos.arm.khaos.io.TimeSeries;
import org.apache.log4j.Logger;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class PrometheusClient {

    private static final Logger LOG = Logger.getLogger(PrometheusClient.class);
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

    public TimeSeries queryRange(String query, long start, long end, int step) throws IOException {

        TimeSeries ts = TimeSeries.create(start, end);

        long current = start;
        long iterations = (end - start) / LIMIT;
        for (int i = 0; i <= iterations; i++) {

            long last = current + (end - start) % LIMIT;
            if (i < iterations) last = current + LIMIT;

            Matrix matrix = this.service.queryRange(query, current, last, step, 120).execute().body();
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

    public TimeSeries queryRange(String query, long start, long end) throws IOException {

       return this.queryRange(query, start, end, 1);
    }
}
