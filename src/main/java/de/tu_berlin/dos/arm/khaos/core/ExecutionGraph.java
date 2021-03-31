package de.tu_berlin.dos.arm.khaos.core;

import de.tu_berlin.dos.arm.khaos.clients.flink.responses.Checkpoints;
import de.tu_berlin.dos.arm.khaos.core.Context.StreamingJob;
import de.tu_berlin.dos.arm.khaos.io.IOManager;
import de.tu_berlin.dos.arm.khaos.io.ReplayCounter.Listener;
import de.tu_berlin.dos.arm.khaos.io.TimeSeries;
import de.tu_berlin.dos.arm.khaos.modeling.AnomalyDetector;
import de.tu_berlin.dos.arm.khaos.utils.SequenceFSM;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.Logger;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple6;
import smile.data.DataFrame;
import smile.data.formula.Formula;
import smile.regression.LinearModel;
import smile.regression.OLS;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public enum ExecutionGraph implements SequenceFSM<Context, ExecutionGraph> {

    START {

        public ExecutionGraph runStage(Context context) throws Exception {

            /*for (Tuple6<Double, Long, Double, Double, Long, Double> current : context.IOManager.fetchMetrics()) {
                LOG.info(current);
            }*/

            return RECORD;
        }
    },
    RECORD {

        public ExecutionGraph runStage(Context context) {

            // saves events from kafka consumer topic to database for a user defined time
            //context.IOManager.recordKafkaToDatabase(context.consumerTopic, context.timeLimit, 100000);
            //context.IOManager.extractWorkload();
            //LOG.info(context.IOManager.getWorkload().size());
            //context.IOManager.extractFailureScenario(0.1f);
            for (Tuple3<Integer, Long, Integer> current : context.IOManager.getFailureScenario()) {
                LOG.info(current._1() + " " + current._2() + " " + current._3());
            }

            return DEPLOY;
        }
    },
    DEPLOY {

        public ExecutionGraph runStage(Context context) throws Exception {

            // deploy multiple experiments
            for (StreamingJob job : context.experiments) {

                // start job and set JobId, Operator IDs, and Sink Operator
                job.setJobId(context.clientsManager.startJob(job.getProgramArgs()));
                job.setOperatorIds(context.clientsManager.getOperatorIds(job.getJobId()));
                job.setSinkId(context.clientsManager.getSinkOperatorId(job.getJobId(), context.sinkRegex));
            }
            return REGISTER;
        }
    },
    REGISTER {

        public ExecutionGraph runStage(Context context) {

            context.IOManager.initMetrics();

            // register points for failure injection with counter manager
            context.IOManager.getFailureScenario().forEach(point -> {

                //context.eventsManager.registerListener(new Listener(point, (avgThr) -> {
                context.IOManager.registerListener(new Listener(point._1(), () -> {

                    // inject failure in all experiments
                    for (StreamingJob job : context.experiments) {

                        try {

                            long stopTs = Instant.now().getEpochSecond();
                            long startTs = stopTs - context.averagingWindow;
                            double avgThr = context.clientsManager.getThroughput(job.getJobId(), startTs, stopTs).average();
                            double avgLat = context.clientsManager.getLatency(job.getJobId(), job.getSinkId(), startTs, stopTs).average();
                            context.clientsManager.injectFailure(job.getJobId(), job.getSinkId());
                            long chkLast = context.clientsManager.getLastCheckpoint(job.getJobId());

                            context.IOManager.addMetrics(job.getConfig(), stopTs, avgThr, avgLat, chkLast);
                        }
                        catch (Exception e) {

                            LOG.error(e.fillInStackTrace());
                        }
                    }
                }));
            });

            return REPLAY;
        }
    },
    REPLAY {

        public ExecutionGraph runStage(Context context) {

            // record start time of experiment
            StreamingJob.startTs = Instant.now().getEpochSecond();

            // start generator
            CountDownLatch latch = new CountDownLatch(2);
            BlockingQueue<List<String>> queue = new ArrayBlockingQueue<>(60);
            AtomicBoolean isDone = new AtomicBoolean(false);
            // start reading from file into queue and then into kafka
            CompletableFuture
                .runAsync(context.IOManager.databaseToQueue(queue))
                .thenRun(() -> {
                    isDone.set(true);
                    latch.countDown();
                });
            CompletableFuture
                .runAsync(context.IOManager.queueToKafka(queue, StreamingJob.consumerTopic, isDone))
                .thenRun(latch::countDown);
            // wait till full workload has been replayed
            try {
                latch.await();
            }
            catch (InterruptedException e) {

                e.printStackTrace();
            }
            // record end time of experiment
            StreamingJob.stopTs = Instant.now().getEpochSecond();
            LOG.info(context.experiments);

            return DELETE;
        }
    },
    DELETE {

        public ExecutionGraph runStage(Context context) throws Exception {

            context.IOManager.initChkSums();

            for (StreamingJob job : context.experiments) {

                // save checkpoint metrics
                Checkpoints checkpoints = context.clientsManager.flink.getCheckpoints(job.getJobId());
                context.IOManager.addChkSum(
                    job.getConfig(),
                    checkpoints.summary.endToEndDuration.min,
                    checkpoints.summary.endToEndDuration.avg,
                    checkpoints.summary.endToEndDuration.max,
                    checkpoints.summary.stateSize.min,
                    checkpoints.summary.stateSize.avg,
                    checkpoints.summary.stateSize.max);

                // Stop experimental job
                context.clientsManager.flink.stopJob(job.getJobId());
            }
            return MEASURE;
        }
    },
    MEASURE {

        public ExecutionGraph runStage(Context context) throws Exception {

            int numOfCores = Runtime.getRuntime().availableProcessors();
            final ExecutorService service = Executors.newFixedThreadPool(numOfCores);
            final CountDownLatch latch = new CountDownLatch(context.numOfConfigs * context.numFailures);

            LOG.info("Starting measure failure durations");
            for (StreamingJob job : context.experiments) {
                
                TimeSeries thrTs = context.clientsManager.getThroughput(job.getJobId(), StreamingJob.startTs, StreamingJob.stopTs);
                TimeSeries lagTs = context.clientsManager.getConsumerLag(job.getJobId(), StreamingJob.startTs, StreamingJob.stopTs);

                for (long timestamp : context.IOManager.fetchMetrics(job.getConfig())) {

                    service.submit(() -> {

                        AnomalyDetector detector = new AnomalyDetector(Arrays.asList(thrTs, lagTs));
                        detector.fit(timestamp, 1000);
                        double recTime = detector.measure(timestamp, 600);
                        context.IOManager.updateMetrics(job.getConfig(), timestamp, recTime);
                        latch.countDown();
                    });
                }
            }
            latch.await();
            LOG.info("Finished measure failure durations");

            return STOP;
        }
    },
    MODEL {

        public ExecutionGraph runStage(Context context) {

            // get values from experiments and fit models
            //List<Double> durHats = new ArrayList<>();
            //List<Double> avgLats = new ArrayList<>();
            //List<Double> configs = new ArrayList<>();
            //List<Double> avgThrs = new ArrayList<>();

            //1000, metrics=[(1615553475,486,519,509.3961598342439),(1615575579,24148,604,708.3158652473526),(1615580776,32276,1072,657.5180209847384),(1615594541,12676,316,692.6964472266923),(1615607810,18647,1631,746.064014726303),(1615612818,39559,1716,732.4653452115994),(1615615272,45640,1587,728.3063727597462),(1615616397,52404,691,703.7754790347279),(1615626108,28241,361,592.8236067762407),(1615629999,6440,1545,639.908476059619)]
            //14222, metrics=[(1615553475,486,4063,427.65456889713323),(1615575580,24148,10703,346.991175756106),(1615580776,32276,13367,408.372757515638),(1615594542,12676,3894,372.6481512836444),(1615607811,18647,8710,371.6707021516819),(1615612818,39559,7498,363.11667478441007),(1615615273,45640,1168,435.0748122712702),(1615616398,52404,107,499.51542770030886),(1615626108,28241,5173,405.85926002679867),(1615630000,6440,8721,385.4238575273178)]
            //27444, metrics=[(1615553475,486,17418,384.0473821392883),(1615575580,24148,174,336.9497724260603),(1615580776,32276,16058,394.78158133370533),(1615594542,12676,12502,338.2852156629594),(1615607811,18647,17136,382.8517433749481),(1615612819,39559,7981,405.1102983341661),(1615615273,45640,23998,406.08197558837077),(1615616398,52404,23281,432.6134357642494),(1615626108,28241,14737,374.16409717445754),(1615630000,6440,22048,358.5546714808062)]
            //40666, metrics=[(1615553476,486,19190,411.2392020494835),(1615575580,24148,724,350.45593738239074),(1615580777,32276,8148,359.50716291155135),(1615594543,12676,20587,360.03604582219424),(1615607811,18647,30535,370.20797293526783),(1615612819,39559,2937,402.8977791922433),(1615615273,45640,2892,396.6322476713364),(1615616398,52404,13978,424.05018823566627),(1615626109,28241,3207,370.9127494330422),(1615630000,6440,25341,344.95809754026294)]
            //53888, metrics=[(1615553476,486,30212,358.87059908134995),(1615575583,24148,39763,359.0576610882021),(1615580777,32276,46314,368.6942896035026),(1615594544,12676,50245,343.36733653933504),(1615607812,18647,5128,374.31800208614516),(1615612821,39559,19876,423.7062922379503),(1615615274,45640,39731,422.16004705587494),(1615616400,52404,32760,444.8718024472462),(1615626112,28241,39110,401.51179438492784),(1615630000,6440,451,350.9274523155238)]
            //67110, metrics=[(1615553477,486,6140,400.3784057008864),(1615575583,24148,66738,328.8327450166113),(1615580777,32276,38560,405.8959715580227),(1615594544,12676,25284,312.8148455952489),(1615607812,18647,5580,360.71102449030576),(1615612822,39559,13399,342.2290069478691),(1615615274,45640,22570,452.62012012773175),(1615616400,52404,40861,475.6847352696416),(1615626114,28241,17353,325.0940814350927),(1615630001,6440,9880,335.40911601627386)]
            //80332, metrics=[(1615553477,486,16057,379.8206186896543),(1615575584,24148,55389,369.9158851395414),(1615580778,32276,57925,391.17985914870354),(1615594545,12676,38417,362.02574771108027),(1615607813,18647,2929,369.7720098653901),(1615612822,39559,56788,329.85608538757526),(1615615275,45640,32536,444.77686864909936),(1615616401,52404,2439,451.6951215877089),(1615626117,28241,3348,362.1312091611945),(1615630001,6440,21158,362.28810175391925)]
            //93554, metrics=[(1615553478,486,18772,432.76034089655576),(1615575584,24148,46300,343.0631217069404),(1615580779,32276,56039,361.1931259814291),(1615594546,12676,79606,368.16434810803185),(1615607814,18647,2778,343.4942891573985),(1615612823,39559,8409,395.32580890845617),(1615615275,45640,20697,377.2720323733713),(1615616401,52404,42974,409.8383304431193),(1615626118,28241,19899,385.4324315473487),(1615630003,6440,80578,330.72736731240917)]
            //106776, metrics=[(1615553479,486,18095,395.9111097975823),(1615575588,24148,103681,347.4166738313694),(1615580780,32276,3533,376.6754793186124),(1615594547,12676,94552,357.7394957684995),(1615607816,18647,46832,371.991839538777),(1615612824,39559,56832,335.6206399404329),(1615615276,45640,64955,366.0552943030069),(1615616403,52404,95077,367.96461856880063),(1615626118,28241,97024,347.1246869160091),(1615630004,6440,14157,370.0214071178753)]
            //119998, metrics=[(1615553479,486,3803,398.30174382105224),(1615575588,24148,44558,345.90363228123056),(1615580781,32276,104701,349.3624798847591),(1615594548,12676,68195,397.39246173237643),(1615607816,18647,46481,371.2291115795655),(1615612826,39559,109396,394.2255610976108),(1615615276,45640,53534,410.4948511472176),(1615616403,52404,45751,388.5324553936423),(1615626120,28241,34443,362.0225903076983),(1615630005,6440,55563,350.8563963424328)]

            /*for (StreamingJob streamingJob : context.experiments) {

                // populate model dataset
                for (FailureMetrics failureMetrics : streamingJob.failureMetricsList) {

                    // test checkpoint interval and then normalize the recovery time
                    if (streamingJob.getConfig() > 5000) {

                        double chkDist = failureMetrics.timestamp - failureMetrics.chkLast;
                        if (chkDist < 0) chkDist = streamingJob.getConfig();
                        durHats.add(failureMetrics.getDuration() * (streamingJob.getConfig() / chkDist));
                    }
                    // dont worry about normalizing for these short checkpoint intervals
                    else durHats.add(streamingJob.getConfig());
                    avgLats.add(failureMetrics.avgLat);
                    configs.add(streamingJob.getConfig());
                    avgThrs.add(failureMetrics.avgThr);
                }
            }*/

            List<List<Double>> confThr_apache = Arrays.asList(
                Arrays.asList(1000D, 486D), Arrays.asList(1000D, 24148D), Arrays.asList(1000D, 32276D), Arrays.asList(1000D, 12676D), Arrays.asList(1000D, 18647D), Arrays.asList(1000D, 39559D), Arrays.asList(1000D, 45640D), Arrays.asList(1000D, 52404D), Arrays.asList(1000D, 28241D), Arrays.asList(1000D, 6440D),
                Arrays.asList(14222D, 486D), Arrays.asList(14222D, 24148D), Arrays.asList(14222D, 32276D), Arrays.asList(14222D, 12676D), Arrays.asList(14222D, 18647D), Arrays.asList(14222D, 39559D), Arrays.asList(14222D, 45640D), Arrays.asList(14222D, 52404D), Arrays.asList(14222D, 28241D), Arrays.asList(14222D, 6440D),
                Arrays.asList(27444D, 486D), Arrays.asList(27444D, 24148D), Arrays.asList(27444D, 32276D), Arrays.asList(27444D, 12676D), Arrays.asList(27444D, 18647D), Arrays.asList(27444D, 39559D), Arrays.asList(27444D, 45640D), Arrays.asList(27444D, 52404D), Arrays.asList(27444D, 28241D), Arrays.asList(27444D, 6440D),
                Arrays.asList(40666D, 486D), Arrays.asList(40666D, 24148D), Arrays.asList(40666D, 32276D), Arrays.asList(40666D, 12676D), Arrays.asList(40666D, 18647D), Arrays.asList(40666D, 39559D), Arrays.asList(40666D, 45640D), Arrays.asList(40666D, 52404D), Arrays.asList(40666D, 28241D), Arrays.asList(40666D, 6440D),
                Arrays.asList(53888D, 486D), Arrays.asList(53888D, 24148D), Arrays.asList(53888D, 32276D), Arrays.asList(53888D, 12676D), Arrays.asList(53888D, 18647D), Arrays.asList(53888D, 39559D), Arrays.asList(53888D, 45640D), Arrays.asList(53888D, 52404D), Arrays.asList(53888D, 28241D), Arrays.asList(53888D, 6440D),
                Arrays.asList(67110D, 486D), Arrays.asList(67110D, 24148D), Arrays.asList(67110D, 32276D), Arrays.asList(67110D, 12676D), Arrays.asList(67110D, 18647D), Arrays.asList(67110D, 39559D), Arrays.asList(67110D, 45640D), Arrays.asList(67110D, 52404D), Arrays.asList(67110D, 28241D), Arrays.asList(67110D, 6440D),
                Arrays.asList(80332D, 486D), Arrays.asList(80332D, 24148D), Arrays.asList(80332D, 32276D), Arrays.asList(80332D, 12676D), Arrays.asList(80332D, 18647D), Arrays.asList(80332D, 39559D), Arrays.asList(80332D, 45640D), Arrays.asList(80332D, 52404D), Arrays.asList(80332D, 28241D), Arrays.asList(80332D, 6440D),
                Arrays.asList(93554D, 486D), Arrays.asList(93554D, 24148D), Arrays.asList(93554D, 32276D), Arrays.asList(93554D, 12676D), Arrays.asList(93554D, 18647D), Arrays.asList(93554D, 39559D), Arrays.asList(93554D, 45640D), Arrays.asList(93554D, 52404D), Arrays.asList(93554D, 28241D), Arrays.asList(93554D, 6440D),
                Arrays.asList(106776D, 486D), Arrays.asList(106776D, 24148D), Arrays.asList(106776D, 32276D), Arrays.asList(106776D, 12676D), Arrays.asList(106776D, 18647D), Arrays.asList(106776D, 39559D), Arrays.asList(106776D, 45640D), Arrays.asList(106776D, 52404D), Arrays.asList(106776D, 28241D), Arrays.asList(106776D, 6440D),
                Arrays.asList(119998D, 486D), Arrays.asList(119998D, 24148D), Arrays.asList(119998D, 32276D), Arrays.asList(119998D, 12676D), Arrays.asList(119998D, 18647D), Arrays.asList(119998D, 39559D), Arrays.asList(119998D, 45640D), Arrays.asList(119998D, 52404D), Arrays.asList(119998D, 28241D), Arrays.asList(119998D, 6440D));

            List<Double> avgLats_apache = Arrays.asList(
                509D, 708D, 657D, 692D, 746D, 732D, 728D, 703D, 592D, 639D,
                427D, 346D, 408D, 372D, 371D, 363D, 435D, 499D, 405D, 385D,
                384D, 336D, 394D, 338D, 382D, 405D, 406D, 432D, 374D, 358D,
                411D, 350D, 359D, 360D, 370D, 402D, 396D, 424D, 370D, 344D,
                358D, 359D, 368D, 343D, 374D, 423D, 422D, 444D, 401D, 350D,
                400D, 328D, 405D, 312D, 360D, 342D, 452D, 475D, 325D, 335D,
                379D, 369D, 391D, 362D, 369D, 329D, 444D, 451D, 362D, 362D,
                432D, 343D, 361D, 368D, 343D, 395D, 377D, 409D, 385D, 330D,
                395D, 347D, 376D, 357D, 371D, 335D, 366D, 367D, 347D, 370D,
                398D, 345D, 349D, 397D, 371D, 394D, 410D, 388D, 362D, 350D
            );

            List<List<Double>> x = Arrays.asList(
                Arrays.asList(509D,1000D, 486D), Arrays.asList(708D,1000D, 24148D), Arrays.asList(657D,1000D, 32276D), Arrays.asList(692D,1000D, 12676D), Arrays.asList(746D,1000D, 18647D), Arrays.asList(732D,1000D, 39559D), Arrays.asList(728D,1000D, 45640D), Arrays.asList(703D,1000D, 52404D), Arrays.asList(592D,1000D, 28241D), Arrays.asList(639D,1000D, 6440D),
                Arrays.asList(427D,14222D, 486D), Arrays.asList(346D,14222D, 24148D), Arrays.asList(408D,14222D, 32276D), Arrays.asList(372D,14222D, 12676D), Arrays.asList(371D,14222D, 18647D), Arrays.asList(363D,14222D, 39559D), Arrays.asList(435D,14222D, 45640D), Arrays.asList(499D,14222D, 52404D), Arrays.asList(405D,14222D, 28241D), Arrays.asList(385D,14222D, 6440D),
                Arrays.asList(384D,27444D, 486D), Arrays.asList(336D,27444D, 24148D), Arrays.asList(394D,27444D, 32276D), Arrays.asList(338D,27444D, 12676D), Arrays.asList(382D,27444D, 18647D), Arrays.asList(405D,27444D, 39559D), Arrays.asList(406D,27444D, 45640D), Arrays.asList(432D,27444D, 52404D), Arrays.asList(374D,27444D, 28241D), Arrays.asList(358D,27444D, 6440D),
                Arrays.asList(411D,40666D, 486D), Arrays.asList(350D,40666D, 24148D), Arrays.asList(359D,40666D, 32276D), Arrays.asList(360D,40666D, 12676D), Arrays.asList(370D,40666D, 18647D), Arrays.asList(402D,40666D, 39559D), Arrays.asList(396D,40666D, 45640D), Arrays.asList(424D,40666D, 52404D), Arrays.asList(370D,40666D, 28241D), Arrays.asList(344D,40666D, 6440D),
                Arrays.asList(358D,53888D, 486D), Arrays.asList(359D,53888D, 24148D), Arrays.asList(368D,53888D, 32276D), Arrays.asList(343D,53888D, 12676D), Arrays.asList(374D,53888D, 18647D), Arrays.asList(423D,53888D, 39559D), Arrays.asList(422D,53888D, 45640D), Arrays.asList(444D,53888D, 52404D), Arrays.asList(401D,53888D, 28241D), Arrays.asList(350D,53888D, 6440D),
                Arrays.asList(400D,67110D, 486D), Arrays.asList(328D,67110D, 24148D), Arrays.asList(405D,67110D, 32276D), Arrays.asList(312D,67110D, 12676D), Arrays.asList(360D,67110D, 18647D), Arrays.asList(342D,67110D, 39559D), Arrays.asList(452D,67110D, 45640D), Arrays.asList(475D,67110D, 52404D), Arrays.asList(325D,67110D, 28241D), Arrays.asList(335D,67110D, 6440D),
                Arrays.asList(379D,80332D, 486D), Arrays.asList(369D,80332D, 24148D), Arrays.asList(391D,80332D, 32276D), Arrays.asList(362D,80332D, 12676D), Arrays.asList(369D,80332D, 18647D), Arrays.asList(342D,80332D, 39559D), Arrays.asList(444D,80332D, 45640D), Arrays.asList(451D,80332D, 52404D), Arrays.asList(362D,80332D, 28241D), Arrays.asList(362D,80332D, 6440D),
                Arrays.asList(432D,93554D, 486D), Arrays.asList(343D,93554D, 24148D), Arrays.asList(361D,93554D, 32276D), Arrays.asList(368D,93554D, 12676D), Arrays.asList(343D,93554D, 18647D), Arrays.asList(395D,93554D, 39559D), Arrays.asList(377D,93554D, 45640D), Arrays.asList(409D,93554D, 52404D), Arrays.asList(385D,93554D, 28241D), Arrays.asList(330D,93554D, 6440D),
                Arrays.asList(395D,106776D, 486D), Arrays.asList(347D,106776D, 24148D), Arrays.asList(376D,106776D, 32276D), Arrays.asList(357D,106776D, 12676D), Arrays.asList(371D,106776D, 18647D), Arrays.asList(335D,106776D, 39559D), Arrays.asList(366D,106776D, 45640D), Arrays.asList(367D,106776D, 52404D), Arrays.asList(347D,106776D, 28241D), Arrays.asList(370D,106776D, 6440D),
                Arrays.asList(398D,119998D, 486D), Arrays.asList(345D,119998D, 24148D), Arrays.asList(349D,119998D, 32276D), Arrays.asList(397D,119998D, 12676D), Arrays.asList(371D,119998D, 18647D), Arrays.asList(394D,119998D, 39559D), Arrays.asList(410D,119998D, 45640D), Arrays.asList(388D,119998D, 52404D), Arrays.asList(362D,119998D, 28241D), Arrays.asList(350D,119998D, 6440D));
            
            List<Double> durHats = Arrays.asList();

            double [][] arr_data = new double[x.size()][];
            for (int i =0; i < x.size(); i++) {

                arr_data[i] = ArrayUtils.toPrimitive(x.get(i).toArray(new Double[0]));
            }

            DataFrame dataDf = DataFrame.of(arr_data, "lat", "conf", "thr");
            System.out.println(dataDf);
            LinearModel model = OLS.fit(Formula.lhs("lat"), dataDf);
            System.out.println(model);
            System.out.println(Arrays.toString(model.coefficients()));
            System.out.println(model.predict(new double[]{1D, 50000D, 30000D}));


            // fit models for performance and availability
            context.performance.fit(avgLats_apache, confThr_apache);
            //context.availability.fit(durHats, Arrays.asList(configs, avgThrs));
            LOG.info(context.performance.predict(Arrays.asList(50000D, 30000D)));


            // Log R^2 and P-values for models
            LOG.info(context.performance.calculateRSquared());
            LOG.info(Arrays.toString(context.performance.calculatePValues().toArray()));
            //LOG.info(context.availability.calculateRSquared());
            //LOG.info(Arrays.toString(context.availability.calculatePValues().toArray()));

            return OPTIMIZE;
        }
    },
    OPTIMIZE {

        public ExecutionGraph runStage(Context context) throws Exception {

            context.targetJob.setSinkId(context.clientsManager.getSinkOperatorId(context.jobId, context.sinkRegex));

            final AtomicBoolean isWarmupTime = new AtomicBoolean(false);
            final StopWatch stopWatch = new StopWatch();
            while (true) {

                try {
                    stopWatch.start();

                    long stopTs = Instant.now().getEpochSecond();
                    long startTs = stopTs - context.averagingWindow;

                    // read current metrics
                    long chkLast = context.clientsManager.getLastCheckpoint(context.targetJob.getJobId());
                    double avgLat = context.clientsManager.getLatency(context.jobId, context.targetJob.getSinkId(), startTs, stopTs).average();
                    double avgThr = context.clientsManager.getThroughput(context.jobId, startTs, stopTs).average();
                    // TODO retrieve values from availability models
                    double recTime = context.availability.predict(Arrays.asList());

                    // evaluate metrics based on constraints
                    if (context.constraintPerformance < avgLat && recTime < context.constraintAvailability) {

                        // TODO perform optimization step for performance

                    }
                    else if (context.constraintAvailability < recTime && avgLat < context.constraintPerformance) {

                        // TODO perform optimization step for availability

                    }
                    else if (context.constraintAvailability < recTime && context.constraintPerformance < avgLat) {

                        LOG.error(String.format("Unable to optimize, %s < %f and %d < %f",
                            context.constraintPerformance, avgLat, context.constraintAvailability, recTime));
                    }

                    // wait until next interval is reached
                    long current = stopWatch.getTime(TimeUnit.SECONDS);
                    while (current < context.constraintInterval) {

                        current = stopWatch.getTime(TimeUnit.SECONDS);
                        Thread.sleep(100);
                    }
                    stopWatch.reset();
                }
                catch (Exception e) {

                    LOG.error(e.fillInStackTrace());
                    break;
                }
            }
            return STOP;
        }
    },
    STOP {

        public ExecutionGraph runStage(Context context) {

            return this;
        }
    };

    private static final Logger LOG = Logger.getLogger(ExecutionGraph.class);

    public static void start() throws Exception {

        LOG.info("START");
        ExecutionGraph.START.run(ExecutionGraph.class, Context.get);
    }
}
