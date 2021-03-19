package de.tu_berlin.dos.arm.khaos.common.data;

import java.util.*;

public class TimeSeries {

    public static TimeSeries create(long startTimestamp, long endTimestamp) {

        LinkedList<Observation> observations = new LinkedList<>();
        for (long i = startTimestamp; i <= endTimestamp; i++) {

            observations.add(new Observation(i, Double.NaN));
        }
        return new TimeSeries(observations);
    }

    public final LinkedList<Observation> observations;

    private TimeSeries(LinkedList<Observation> observations) {

        Collections.sort(observations);
        this.observations = observations;
    }

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

    @Override
    public String toString() {

        return "TimeSeries{" +
                "observations=" + observations +
                ", count=" + observations.size() +
                '}';
    }
}
