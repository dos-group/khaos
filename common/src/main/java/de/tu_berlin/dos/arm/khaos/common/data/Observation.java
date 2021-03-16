package de.tu_berlin.dos.arm.khaos.common.data;

public class Observation implements Comparable<Observation> {

    public final long timestamp;
    public final double value;

    public Observation(long timestamp, double value) {

        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public String toString() {
        return "Observation{" +
                "timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }

    @Override
    public int compareTo(Observation that) {

        return Long.compare(this.timestamp, that.timestamp);
    }
}
