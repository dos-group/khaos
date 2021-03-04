package de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import java.util.List;

public class Vertices {

    public class Node {

        @SerializedName("id")
        @Expose
        public String id;
        @SerializedName("description")
        @Expose
        public String description;

    }

    static public class Plan {

        @SerializedName("jid")
        @Expose
        public String jid;
        @SerializedName("name")
        @Expose
        public String name;
        @SerializedName("nodes")
        @Expose
        public List<Node> nodes = null;

    }

    @SerializedName("plan")
    @Expose
    public Plan plan;

}
