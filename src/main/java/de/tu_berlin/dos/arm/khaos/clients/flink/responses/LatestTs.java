package de.tu_berlin.dos.arm.khaos.clients.flink.responses;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class LatestTs {

    @SerializedName("now")
    @Expose
    public long now;
}
