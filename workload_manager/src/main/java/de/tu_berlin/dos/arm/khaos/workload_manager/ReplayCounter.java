package de.tu_berlin.dos.arm.khaos.workload_manager;

import org.apache.log4j.Logger;
import scala.Tuple2;
import scala.Tuple3;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ReplayCounter {

    /******************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************/

    public static class Listener {

        private static final Logger LOG = Logger.getLogger(Listener.class);
        private static final ExecutorService service = Executors.newSingleThreadExecutor();

        private final int second;
        private final int throughput;
        private final Consumer<Integer> callback;

        public Listener(Tuple2<Integer, Integer> point, Consumer<Integer> callback) {

            this.second = point._1();
            this.throughput = point._2();
            this.callback = callback;
        }

        public void update() {

            service.execute(() -> {
                try {

                    this.callback.accept(this.throughput);
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
