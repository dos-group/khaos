package de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class TaskManagers {

    public static class TaskManager {

        @SerializedName("host")
        @Expose
        public String host;
        @SerializedName("status")
        @Expose
        public String status;
        @SerializedName("taskmanager-id")
        @Expose
        public String taskManagerId;

    }

    @SerializedName("id")
    @Expose
    public String jobId;
    @SerializedName("taskmanagers")
    @Expose
    public List<TaskManager> taskManagers = null;
}
