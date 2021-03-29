package de.tu_berlin.dos.arm.khaos.events;

import org.apache.log4j.Logger;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class ReplayCounter {

    /******************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************/

    public static class Listener {

        private static final Logger LOG = Logger.getLogger(Listener.class);
        private static final ExecutorService service = Executors.newSingleThreadExecutor();

        private final int second;
        private final int avgThr;
        private final Consumer<Integer> callback;

        public Listener(Tuple3<Integer, Long, Integer> point, Consumer<Integer> callback) {

            this.second = point._1();
            this.avgThr = point._3();
            this.callback = callback;
        }

        public void update() {

            service.execute(() -> {
                try {

                    this.callback.accept(this.avgThr);
                }
                catch (Throwable t) {

                    LOG.error(t);
                }
            });
        }

        public int getSecond() {

            return this.second;
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

    public void register(Listener listener) {

        this.listeners.add(listener);
    }

    /******************************************************************************
     * INSTANCE BEHAVIOUR
     ******************************************************************************/

    public void register(List<Listener> listeners) {

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
        for (Listener listener : this.listeners) {

            if (newVal == listener.getSecond()) listener.update();
        }

    }
}
