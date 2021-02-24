package de.tu_berlin.dos.arm.khaos.common.api_clients.prometheus.responses;

import java.util.List;
import java.util.Map;

public class Vector {

    public static class VectorData {

        public static class VectorResult {

            public Map<String, String> metric;
            public List<Float> value;

            @Override
            public String toString() {

                return String.format("metric: %s\nvalue: %s", metric.toString(), value == null ? "" : value.toString());
            }
        }

        public String resultType;
        public List<VectorResult> result;
    }

    public String status;
    public VectorData data;
}
