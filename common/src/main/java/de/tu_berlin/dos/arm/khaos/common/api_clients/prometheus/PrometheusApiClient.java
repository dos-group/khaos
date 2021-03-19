package de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus;

import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.responses.Matrix;
import de.tu_berlin.dos.arm.khaos.common.data.Observation;
import de.tu_berlin.dos.arm.khaos.common.data.TimeSeries;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class PrometheusApiClient {

    private final PrometheusRest service;
    private final int limit;
    private final int step;
    private final int timeout;

    public PrometheusApiClient(String baseUrl, int step, int timeout) {

        Retrofit retrofit =
            new Retrofit.Builder()
                .baseUrl("http://" + baseUrl + "/")
                .addConverterFactory(GsonConverterFactory.create())
                .build();
        this.service = retrofit.create(PrometheusRest.class);
        this.limit = 11000;
        this.step = step;
        this.timeout = timeout;
    }

    public TimeSeries queryRange(String query, long start, long end) throws IOException {

        TimeSeries ts = TimeSeries.create(start, end);
        long current = start;
        while (current < end) {

            Matrix matrix = this.service.queryRange(query, current, end, this.step, this.timeout).execute().body();
            List<List<String>> values = Objects.requireNonNull(matrix).data.result.get(0).values;
            for (List<String> strings : values) {

                long timestamp = Long.parseLong(strings.get(0));
                double value = Double.parseDouble(strings.get(1));
                ts.setObservation(new Observation(timestamp, value));
            }
            current += this.limit;
        }
        return ts;
    }
}
