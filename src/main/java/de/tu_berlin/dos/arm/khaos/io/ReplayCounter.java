package de.tu_berlin.dos.arm.khaos.io;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplayCounter {

    /******************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************/

    public static class Listener {

        private static final Logger LOG = Logger.getLogger(Listener.class);
        private static final ExecutorService service = Executors.newSingleThreadExecutor();

        private final int index;
        private final Runnable callback;

        public Listener(int index, Runnable callback) {

            this.index = index;
            this.callback = callback;
        }

        public void update() {

            service.execute(() -> {
                try {

                    this.callback.run();
                }
                catch (Throwable t) {

                    LOG.error(t);
                }
            });
        }

        public int getIndex() {

            return this.index;
        }
    }

    /******************************************************************************
     * CLASS VARIABLES
     ******************************************************************************/

    private static final Logger LOG = Logger.getLogger(ReplayCounter.class);

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    private final AtomicInteger counter = new AtomicInteger(1);
    private final List<Listener> listeners = new ArrayList<>();

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public ReplayCounter() { }

    /******************************************************************************
     * INSTANCE BEHAVIOUR
     ******************************************************************************/

    public void register(Listener listener) {

        this.listeners.add(listener);
    }

    public int getCounter() {

        return this.counter.get();
    }

    public void resetCounter(int counter) {

        this.counter.set(counter);
    }

    public void incrementCounter() {

        int newVal = this.counter.incrementAndGet();
        for (Listener listener : this.listeners) {

            if (newVal == listener.getIndex()) listener.update();
        }

    }
}
