package de.tu_berlin.dos.arm.khaos.workload_manager;

import com.google.gson.JsonParser;
import de.tu_berlin.dos.arm.khaos.common.utils.DateUtil;
import de.tu_berlin.dos.arm.khaos.common.utils.FileReader;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.math3.analysis.interpolation.SplineInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import scala.Tuple2;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public enum WorkloadPlayer { GET;

    /******************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************/

    public interface CounterListener {

        void update(int value);
        int getValue();
    }

    public static class CounterManager implements Runnable {

        private final AtomicInteger counter = new AtomicInteger(1);
        private final List<CounterListener> listeners = new ArrayList<>();

        public void resetCounter() {

            this.counter.set(1);
        }

        public void addObserver(CounterListener listener) {

            this.listeners.add(listener);
        }

        public void addObservers(List<CounterListener> listeners) {

            this.listeners.addAll(listeners);
        }

        public void removeObserver(CounterListener listener) {

            this.listeners.remove(listener);
        }

        public void incrementCounter() {

            int newVal = this.counter.incrementAndGet();
            for (CounterListener listener : this.listeners) {

                if (newVal == listener.getValue()) listener.update(newVal);
            }

        }

        @Override
        public void run() {

        }
    }

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    private final BlockingQueue<List<String>> queue = new ArrayBlockingQueue<>(60);
    private final CounterManager counterManager = new CounterManager();

    /******************************************************************************
     * INSTANCE BEHAVIOURS
     ******************************************************************************/

    private CompletableFuture<PolynomialSplineFunction> startProducer(File inputFile, String tsLabel, boolean isReplay) {

        return CompletableFuture.supplyAsync(() -> {

            // create arrays for interpolation function
            List<Double> x = new ArrayList<>();
            List<Double> y = new ArrayList<>();

            try (Scanner sc = new Scanner(new FileInputStream(inputFile), StandardCharsets.UTF_8)) {
                // initialize loop values
                double xCounter = 0;
                double yCounter = 0;
                List<String> events = null;
                Timestamp head = null;
                // loop reading through file line by line
                while (sc.hasNextLine()) {
                    // extract timestamp from line
                    String line = sc.nextLine();
                    String tsString = JsonParser.parseString(line).getAsJsonObject().get(tsLabel).getAsString();
                    Timestamp current = new Timestamp(DateUtil.provideDateFormat().parse(tsString).getTime());
                    // test if it is the first iteration
                    if (head == null) {

                        xCounter = 1;
                        yCounter = 1;
                        head = current;
                        if (isReplay) {

                            events = new ArrayList<>();
                            events.add(line);
                        }
                    }
                    // test if timestamps match
                    else if (head.compareTo(current) == 0) {

                        yCounter++;

                        if (isReplay) {

                            events.add(line);
                        }
                    }
                    // test if timestamps do not match
                    else if (head.compareTo(current) != 0) {

                        head = current;
                        x.add(xCounter);
                        y.add(yCounter);
                        xCounter++;
                        yCounter = 1;

                        if (isReplay) {

                            queue.put(events);
                            events = new ArrayList<>();
                            events.add(line);
                        }
                    }
                }
            }
            catch (ParseException | IOException | InterruptedException e) {

                e.printStackTrace();
            }
            // convert values to unboxed array type and return interpolate function
            double[] xArr = ArrayUtils.toPrimitive(x.toArray(new Double[0]));
            double[] yArr = ArrayUtils.toPrimitive(y.toArray(new Double[0]));
            return new SplineInterpolator().interpolate(xArr, yArr);
        });
    }

    private CompletableFuture<Void> startConsumer(AtomicBoolean isDone) {

        return CompletableFuture.runAsync(() -> {

            try {
                // wait till queue has items in it
                while (queue.isEmpty()) Thread.sleep(100);
                // initialize and start the stopwatch
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                // execute while file is still being read and queue is not empty
                while (!isDone.get() && !queue.isEmpty()) {
                    // get current time
                    int current = (int) stopWatch.getTime(TimeUnit.SECONDS);
                    // todo produce to kafka producer
                    System.out.println(queue.take().size());
                    // wait until second has passed before continuing next loop
                    while (current < counterManager.counter.get()) {

                        current = (int) stopWatch.getTime(TimeUnit.SECONDS);
                    }
                    counterManager.incrementCounter();
                }
            }
            catch (InterruptedException e) {

                e.printStackTrace();
            }
        });
    }

    public PolynomialSplineFunction analyse(File inputFile, String tsLabel) throws Exception {

        return startProducer(inputFile, tsLabel, false).get();
    }

    public void run(File inputFile, String tsLabel) throws Exception {

        CountDownLatch latch = new CountDownLatch(2);
        queue.removeIf((a) -> true);
        AtomicBoolean isDone = new AtomicBoolean(false);
        // Run futures
        counterManager.resetCounter();
        startProducer(inputFile, tsLabel, true).thenRun(() -> {

            isDone.set(true);
            latch.countDown();
        });
        startConsumer(isDone).thenRun(latch::countDown);
        // wait till producer and consumer has finished
        latch.await();
    }

    public static void main(String[] args) throws Exception {

        // get properties file
        Properties generatorProps = FileReader.GET.read("generator.properties", Properties.class);

        String inputFileName = generatorProps.getProperty("dataset.inputFile");
        File inputFile = FileReader.GET.read(inputFileName, File.class);
        String tsLabel = generatorProps.getProperty("dataset.tsLabel");

        PolynomialSplineFunction workload = WorkloadPlayer.GET.analyse(inputFile, tsLabel);
        System.out.println(workload.derivative());

        CounterListener listener1 = new CounterListener() {

            private final int value = 10;

            @Override
            public void update(int value) {

                System.out.println("CounterListener1: " + this.value + ", " + value);
            }

            @Override
            public int getValue() {

                return this.value;
            }
        };

        CounterListener listener2 = new CounterListener() {

            private final int value = 60;

            @Override
            public void update(int value) {

                System.out.println("CounterListener2: " + this.value + ", " + value);
            }

            @Override
            public int getValue() {

                return this.value;
            }
        };
        WorkloadPlayer.GET.counterManager.addObservers(Arrays.asList(listener1, listener2));
        WorkloadPlayer.GET.run(inputFile, tsLabel);
    }
}
