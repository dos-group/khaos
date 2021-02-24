package de.tu_berlin.dos.arm.khaos.common.api_clients.flink;

import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.StartJob;
import retrofit2.Call;
import retrofit2.http.*;

public interface FlinkRest {

    @POST("v1/jars/{id}/run")
    Call<StartJob> startJob(
        @Path("id") String id,
        @Query("programArg") String programArg,
        @Query("parallelism") int parallelism
    );

    @PATCH("v1/jobs/{id}")
    Call<Void> stopJob(
        @Path("id") String id
    );
}
