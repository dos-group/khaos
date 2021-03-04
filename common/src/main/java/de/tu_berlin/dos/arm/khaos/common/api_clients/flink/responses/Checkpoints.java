package de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Checkpoints {

    public class Completed {

        @SerializedName("status")
        @Expose
        public String status;
        @SerializedName("latest_ack_timestamp")
        @Expose
        public Long latestAckTimestamp;

    }

    static public class Latest {

        @SerializedName("completed")
        @Expose
        public Completed completed;

    }

    @SerializedName("latest")
    @Expose
    public Latest latest;

}
