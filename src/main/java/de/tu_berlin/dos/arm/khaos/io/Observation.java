package de.tu_berlin.dos.arm.khaos.io;

import java.util.Objects;

public class Observation implements Comparable<Observation> {

    public final long timestamp;
    public final double value;

    public Observation(long timestamp, double value) {

        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public int compareTo(Observation that) {

        return Long.compare(this.timestamp, that.timestamp);
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Observation that = (Observation) o;
        return timestamp == that.timestamp;
    }

    @Override
    public int hashCode() {

        return Objects.hash(timestamp);
    }

    @Override
    public String toString() {
        return "Observation{" +
                "timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
