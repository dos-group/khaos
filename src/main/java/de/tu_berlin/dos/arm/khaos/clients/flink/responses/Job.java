package de.tu_berlin.dos.arm.khaos.clients.flink.responses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import de.tu_berlin.dos.arm.khaos.clients.flink.responses.Checkpoints.Latest;
import de.tu_berlin.dos.arm.khaos.clients.flink.responses.Checkpoints.Summary;

public class Job {

    @SerializedName("jobid")
    @Expose
    public String jobId;
}
