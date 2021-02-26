package de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Taskmanager {

    @SerializedName("host")
    @Expose
    public String host;
    @SerializedName("status")
    @Expose
    public String status;
    @SerializedName("taskmanager-id")
    @Expose
    public String taskmanagerId;

}
