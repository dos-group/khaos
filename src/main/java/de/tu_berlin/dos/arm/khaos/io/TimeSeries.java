package de.tu_berlin.dos.arm.khaos.io;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

public class TimeSeries {

    /******************************************************************************
     * STATIC VARIABLES
     ******************************************************************************/

    public static final Logger LOG = Logger.getLogger(TimeSeries.class);

    /******************************************************************************
     * CLASS BEHAVIOURS
     ******************************************************************************/

    public static TimeSeries create(long startTimestamp, long endTimestamp) {

        LinkedList<Observation> observations = new LinkedList<>();
        for (long i = startTimestamp; i <= endTimestamp; i++) {

            observations.add(new Observation(i, Double.NaN));
        }
        return new TimeSeries(observations);
    }

    public static TimeSeries fromCSV(File file, String sep, boolean header) throws Exception {

        List<String> lines = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            boolean headerRead = false;
            while ((line = br.readLine()) != null) {
                if (header && !headerRead) {
                    headerRead = true;
                    continue;
                }
                lines.add(line);
            }
        }
        // create time series using first and last element
        TimeSeries ts =
            TimeSeries.create(
                Long.parseLong(lines.get(0).split(sep)[0]),
                Long.parseLong(lines.get(lines.size() - 1).split(sep)[0]));

        for (String line : lines) {

            String[] values = line.split(sep);
            try {
                ts.setObservation(new Observation(Long.parseLong(values[0]), Double.parseDouble(values[1])));
            }
            catch (Exception e) {

                LOG.error(e.getMessage());
            }
        }
        return ts;
    }

    public static void toCSV(String fileName, TimeSeries ts, String header, String sep) throws IOException {

        File file = new File(fileName);
        if (!file.exists()) file.createNewFile();

        FileWriter fw = new FileWriter(fileName);
        fw.write(header + "\n");
        for (Observation observation : ts.observations) {

            fw.write(observation.timestamp + sep + observation.value + "\n");
        }
        fw.close();
    }

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    public final LinkedList<Observation> observations;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    private TimeSeries(LinkedList<Observation> observations) {

        Collections.sort(observations);
        this.observations = observations;
    }

    /******************************************************************************
     * INSTANCE BEHAVIOUR
     ******************************************************************************/

    public int size() {

        return this.observations.size();
    }

    public TimeSeries resample(int sampleRate, Optional<Integer> limit) {

        long last = this.observations.get(this.observations.size() - 1).timestamp;
        return resample(last, sampleRate, limit);
    }

    public TimeSeries resample(long timestamp, int sampleRate, Optional<Integer> limit) {

        // find timestamp in observations using binary search
        int i = this.getIndex(timestamp);

        LinkedList<Observation> result = new LinkedList<>();
        if (observations.get(i).timestamp == timestamp) {
            // calculate the number of samples in new time series
            int count = limit.orElse(i + 1);
            // iterate backwards of linked list from matching timestamp
            int j = 0;
            ListIterator<Observation> iterator = observations.listIterator(i + 1);
            while(iterator.hasPrevious() && result.size() < count) {
                // retrieve sample
                Observation obs = iterator.previous();
                // test if current sample index is within sample rate
                if (j % sampleRate == 0) {
                    // append valid sample to front of results
                    result.addFirst(obs);
                }
                ++j;
            }
        }
        return new TimeSeries(result);
    }

    public int getIndex(long timestamp) {

        return Collections.binarySearch(observations, new Observation(timestamp, Double.NaN));
    }

    public void setObservation(Observation observation) {

        this.observations.set(this.observations.indexOf(observation), observation);
    }

    public Observation getObservation(long timestamp) {

        int i = getIndex(timestamp);
        return observations.get(i);
    }

    public double[] values() {

        return this.observations.stream().mapToDouble(v -> v.value).toArray();
    }

    public Observation getLast() {
        return this.observations.getLast();
    }

    public double average() {

        double sum = 0;
        int count = 0;
        for (Observation observation : this.observations) {

            if (!Double.isNaN(observation.value)) {

                sum += observation.value;
                count++;
            }
        }
        return sum / count;
    }

    public double min() {

        double min = 0;
        for (Observation observation : this.observations) {

            if (!Double.isNaN(observation.value) && observation.value < min) {

                min = observation.value;
            }
        }
        return min;
    }

    public double max() {

        double max = 0;
        for (Observation observation : this.observations) {

            if (!Double.isNaN(observation.value) && max < observation.value) {

                max = observation.value;
            }
        }
        return max;
    }

    @Override
    public String toString() {

        return "TimeSeries{" +
                "observations=" + observations +
                ", count=" + observations.size() +
                '}';
    }
}
