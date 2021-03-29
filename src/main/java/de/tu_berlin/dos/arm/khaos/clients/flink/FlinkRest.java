package de.tu_berlin.dos.arm.khaos.clients.flink;

import de.tu_berlin.dos.arm.khaos.clients.flink.responses.Checkpoints;
import de.tu_berlin.dos.arm.khaos.clients.flink.responses.Job;
import de.tu_berlin.dos.arm.khaos.clients.flink.responses.TaskManagers;
import de.tu_berlin.dos.arm.khaos.clients.flink.responses.Vertices;
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

    @GET("v1/jobs/{jobId}/plan/")
    Call<Vertices> getVertices(
            @Path("jobId") String jobId
    );

    @GET("v1/jobs/{jobId}/checkpoints/")
    Call<Checkpoints> getCheckpoints(
            @Path("jobId") String jobId
    );

}
