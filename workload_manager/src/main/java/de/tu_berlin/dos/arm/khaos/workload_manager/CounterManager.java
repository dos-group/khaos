package de.tu_berlin.dos.arm.khaos.workload_manager;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CounterManager {

    /******************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************/

    public static class CounterListener {

        private final int value;
        private final Runnable callback;

        public CounterListener(int value, Runnable callback) {

            this.value = value;
            this.callback = callback;
        }

        public void update(int value) {

            this.callback.run();
        }

        public int getValue() {

            return this.value;
        }
    }

    /******************************************************************************
     * CLASS VARIABLES
     ******************************************************************************/

    private static final Logger LOG = Logger.getLogger(CounterManager.class);

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    private final AtomicInteger counter = new AtomicInteger(1);
    private final List<CounterListener> listeners = new ArrayList<>();

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public void register(CounterListener listener) {

        this.listeners.add(listener);
    }

    /******************************************************************************
     * INSTANCE BEHAVIOUR
     ******************************************************************************/

    public void register(List<CounterListener> listeners) {

        this.listeners.addAll(listeners);
    }

    public int getCounter() {

        return this.counter.get();
    }

    public void resetCounter() {

        this.counter.set(1);
    }

    public void incrementCounter() {

        int newVal = this.counter.incrementAndGet();
        for (CounterListener listener : this.listeners) {

            if (newVal == listener.getValue()) listener.update(newVal);
        }

    }
}
