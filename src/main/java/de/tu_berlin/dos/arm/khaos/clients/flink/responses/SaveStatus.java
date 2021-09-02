package de.tu_berlin.dos.arm.khaos.clients.flink.responses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class SaveStatus {

    public static class Status {

        @SerializedName("id")
        @Expose
        public String id;
    }

    public static class Operation {

        @SerializedName("location")
        @Expose
        public String location;
    }

    @SerializedName("status")
    @Expose
    public Status status;

    @SerializedName("operation")
    @Expose
    public Operation operation;
}

