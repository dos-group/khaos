package de.tu_berlin.dos.arm.khaos.clients.flink;

import de.tu_berlin.dos.arm.khaos.clients.flink.responses.*;
import retrofit2.Call;
import retrofit2.http.*;

import java.util.Map;

public interface FlinkRest {

    @POST("v1/jars/{jarId}/run")
    Call<Job> startJob(
        @Path("jarId") String jarId,
        @Query("programArg") String programArg,
        @Query("parallelism") int parallelism
    );

    @POST("v1/jars/{jarId}/run")
    Call<Job> restartJob(
        @Path("jarId") String jarId,
        @Query("savepointPath") String savepointPath,
        @Query("programArg") String programArg,
        @Query("parallelism") int parallelism
    );

    @PATCH("v1/jobs/{jobId}")
    Call<Void> stopJob(
        @Path("jobId") String jobId
    );

    @POST("v1/jobs/{jobId}/savepoints")
    Call<Trigger> saveJob(
        @Path("jobId") String jobId,
        @Body Map<String, Object> body
    );

    @GET("v1/jobs/{jobId}/savepoints/{requestId}")
    Call<SaveStatus> checkStatus(
        @Path("jobId") String jobId,
        @Path("requestId") String requestId
    );

    @GET("v1/jobs/{jobId}/vertices/{vertexId}/taskmanagers")
    Call<TaskManagers> getTaskManagers(
        @Path("jobId") String jobId,
        @Path("vertexId") String vertexId
    );

    @GET("v1/jobs/{jobId}/plan/")
    Call<Vertices> getVertices(
        @Path("jobId") String jobId
    );

    @GET("v1/jobs/{jobId}/")
    Call<LatestTs> getLatestTs(
            @Path("jobId") String jobId
    );

    @GET("v1/jobs/{jobId}/checkpoints/")
    Call<Checkpoints> getCheckpoints(
        @Path("jobId") String jobId
    );

    @GET("v1/jobs/{jobId}")
    Call<Uptime> getUptime(
        @Path("jobId") String jobId
    );

}
