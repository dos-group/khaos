package de.tu_berlin.dos.arm.khaos.common.api_clients.flink;

import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.TaskManagers;
import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.Job;
import retrofit2.Call;
import retrofit2.http.*;

public interface FlinkRest {

    @POST("v1/jars/{jarId}/run")
    Call<Job> startJob(
        @Path("jarId") String jarId,
        @Query("programArg") String programArg,
        @Query("parallelism") int parallelism
    );

    @PATCH("v1/jobs/{jarId}")
    Call<Void> stopJob(
        @Path("jarId") String jarId
    );

    @GET("v1/jobs/{jobId}/vertices/{vertexId}/taskmanagers")
    Call<TaskManagers> getTaskManagers(
        @Path("jobId") String jobId,
        @Path("vertexId") String vertexId
    );
}
