package de.tu_berlin.dos.arm.khaos.clients.prometheus.responses;

import java.util.List;
import java.util.Map;

public class Matrix {

    public static class MatrixData {

        public static class MatrixResult {

            public Map<String, String> metric;
            public List<List<String>> values;

            @Override
            public String toString() {

                return String.format("metric: %s\nvalues: %s", metric.toString(), values == null ? "" : values.toString());
            }
        }

        public String resultType;
        public List<MatrixResult> result;
    }

    public String status;
    public MatrixData data;
}
