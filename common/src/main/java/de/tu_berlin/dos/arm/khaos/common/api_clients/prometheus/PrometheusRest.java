package de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus;

import de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.responses.Matrix;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

public interface PrometheusRest {

    @GET("api/v1/query_range")
    Call<Matrix> queryRange(
        @Query("query") String query,
        @Query("start") long start,
        @Query("end") long end,
        @Query("step") int step,
        @Query("timeout") int timeout
    );
}
