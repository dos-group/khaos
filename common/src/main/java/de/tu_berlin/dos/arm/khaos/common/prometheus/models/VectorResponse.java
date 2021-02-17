package de.tu_berlin.dos.arm.khaos.common.prometheus.models;

import java.util.List;
import java.util.Map;

public class VectorResponse {

    public static class VectorResult {

        public Map<String, String> metric;
        public List<Float> value;

        @Override
        public String toString() {

            return String.format("metric: %s\nvalue: %s", metric.toString(), value == null ? "" : value.toString());
        }
    }

    public static class VectorData {

        public String resultType;
        public List<VectorResult> result;
    }

    public String status;
    public VectorData data;
}
