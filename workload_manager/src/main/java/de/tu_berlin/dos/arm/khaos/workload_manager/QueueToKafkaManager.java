package de.tu_berlin.dos.arm.khaos.workload_manager;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.routing.*;
import de.tu_berlin.dos.arm.khaos.workload_manager.QueueToKafkaManager.WorkerActor.Emit;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class QueueToKafkaManager implements Runnable {

    /******************************************************************************
     * STATIC VARIABLES
     ******************************************************************************/

    private static final Logger LOG = Logger.getLogger(QueueToKafkaManager.class);
    private static final ActorSystem SYSTEM = ActorSystem.create("actor-system");

    /******************************************************************************
     * STATIC INNER CLASSES
     ******************************************************************************/

    public static class MasterActor extends AbstractActor {

        static Props props(int largest, String topic, KafkaProducer<String, String> kafkaProducer) {

            return Props.create(MasterActor.class, largest, topic, kafkaProducer);
        }

        static final class Broadcast {

            public final List<String> eventsList;

            public Broadcast(List<String> eventsList) {

                this.eventsList = eventsList;
            }
        }

        private final Router router;

        private MasterActor(int max, String topic, KafkaProducer<String, String> kafkaProducer) {

            List<Routee> routees = new ArrayList<>();
            for (int i = 1; i <= max; i++) {

                ActorRef worker = getContext().actorOf(WorkerActor.props(i, topic, kafkaProducer));
                getContext().watch(worker);
                routees.add(new ActorRefRoutee(worker));
            }
            this.router = new Router(new BroadcastRoutingLogic(), routees);
        }

        @Override
        public Receive createReceive() {

            return receiveBuilder()
                    .match(Broadcast.class, e -> {
                        LOG.info("here");
                        //router.route(new Emit(e.eventsList), getSender());
                        router.route(new Emit("blip"), getSender());
                    })
                    .matchAny(o -> LOG.error("received unknown message: " + o))
                    .build();
        }
    }

    public static class WorkerActor extends AbstractActor {

        static Props props(int number, String topic, KafkaProducer<String, String> kafkaProducer) {

            return Props.create(WorkerActor.class, number, topic, kafkaProducer);
        }

        static final class Emit {

            //List<String> eventsList;
            String eventsList;

            Emit(/*List<String>*/ String eventsList) {

                this.eventsList = eventsList;
            }
        }

        private String topic;
        private KafkaProducer<String, String> kafkaProducer;
        private int number;

        private WorkerActor() { }

        public WorkerActor(int number, String topic, KafkaProducer<String, String> kafkaProducer) {

            this.number = number;
            this.topic = topic;
            this.kafkaProducer = kafkaProducer;
        }

        @Override
        public Receive createReceive() {

            return receiveBuilder()
                    .match(WorkerActor.Emit.class, e -> {
                        //LOG.info(this.number);
                        //if (this.number >= e.eventsList.size()) {
                            //LOG.info(this.number + " " + e.eventsList.get(this.number));e.eventsList.get(this.number)
                            this.kafkaProducer.send(new ProducerRecord<>(topic, e.eventsList));
                        //}
                    })
                    .matchAny(o -> LOG.error("received unknown message: " + o))
                    .build();
        }
    }

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    private final BlockingQueue<List<String>> queue;
    private final CounterManager counterManager;
    private final int max;
    private final String topic;
    private final KafkaProducer<String, String> kafkaProducer;
    private final AtomicBoolean isDone;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public QueueToKafkaManager(
            BlockingQueue<List<String>> queue, CounterManager counterManager,
            int max, String topic, String brokerList, AtomicBoolean isDone) {

        this.queue = queue;
        this.counterManager = counterManager;
        this.max = max;
        this.topic = topic;
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", brokerList);
        kafkaProps.put("acks", "0");
        kafkaProps.put("retries", 0);
        kafkaProps.put("batch.size", 16384);
        kafkaProps.put("linger.ms", 1000);
        kafkaProps.put("buffer.memory", 33554432);
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.kafkaProducer = new KafkaProducer<>(kafkaProps);
        this.isDone = isDone;
    }

    /******************************************************************************
     * INSTANCE BEHAVIOURS
     ******************************************************************************/

    @Override
    public void run() {

        try {
            ActorRef masterActor = SYSTEM.actorOf(MasterActor.props(max, topic, kafkaProducer));
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
                //System.out.println(queue.take().size());
                //masterActor.tell(new Broadcast(queue.take()), ActorRef.noSender());
                CompletableFuture.runAsync(() -> {
                    try {
                        long startTime = System.currentTimeMillis();
                        queue.take().forEach(e -> kafkaProducer.send(new ProducerRecord<>(topic, e)));
                        long endTime = System.currentTimeMillis();
                        long timeneeded =  endTime - startTime;
                        System.out.println(timeneeded);
                    }
                    catch (InterruptedException ex) {
                        LOG.error(ex);
                    }
                });
                //masterActor.tell(new Broadcast("blip"), ActorRef.noSender());
                // wait until second has passed before continuing next loop
                while (current < counterManager.getCounter()) {

                    current = (int) stopWatch.getTime(TimeUnit.SECONDS);
                }
                counterManager.incrementCounter();
            }
            // terminate the actor system
            SYSTEM.getWhenTerminated().toCompletableFuture().get(3, TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException ex) {

            LOG.error(ex);
        }
    }
}
