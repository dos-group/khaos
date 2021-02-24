package de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus;

import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.responses.KeyVal;
import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.responses.Matrix;
import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.responses.Vector;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

public interface PrometheusRest {

    @GET("api/v1/query")
    Call<Vector> query(
        @Query("query") String query,
        @Query("time") String time,
        @Query("timeout") String timeout
    );

    @GET("api/v1/query_range")
    Call<Matrix> queryRange(
        @Query("query") String query,
        @Query("start") String start,
        @Query("end") String end,
        @Query("step") String step,
        @Query("timeout") String timeout
    );

    @GET("api/v1/series")
    Call<KeyVal> findSeries(
        @Query("match[]") String match,
        @Query("start") String start,
        @Query("end") String end
    );
}
