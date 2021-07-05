package de.tu_berlin.dos.arm.khaos.core;

import de.tu_berlin.dos.arm.khaos.clients.flink.responses.Checkpoints;
import de.tu_berlin.dos.arm.khaos.core.Context.Job;
import de.tu_berlin.dos.arm.khaos.io.IOManager.FailureMetrics;
import de.tu_berlin.dos.arm.khaos.io.IOManager.JobMetrics;
import de.tu_berlin.dos.arm.khaos.io.Observation;
import de.tu_berlin.dos.arm.khaos.io.ReplayCounter.Listener;
import de.tu_berlin.dos.arm.khaos.io.TimeSeries;
import de.tu_berlin.dos.arm.khaos.modeling.AnomalyDetector;
import de.tu_berlin.dos.arm.khaos.modeling.ForecastModel;
import de.tu_berlin.dos.arm.khaos.modeling.Optimization;
import de.tu_berlin.dos.arm.khaos.utils.SequenceFSM;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.Logger;
import scala.*;
import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.formula.Formula;
import smile.data.type.DataTypes;
import smile.data.type.StructField;
import smile.data.type.StructType;
import smile.regression.LinearModel;
import smile.regression.OLS;
import smile.regression.SVR;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.Double;
import java.lang.Long;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum ExecutionManager implements SequenceFSM<Context, ExecutionManager> {

    START {

        public ExecutionManager runStage(Context context) throws Exception {

            /*2021-06-28 18:26:20 INFO  Execution'abd35ed0e22c9f54a08c4a5ed85dd7a7', 1624722285, 57123.342281, 438.153721, 113.0}
            2021-06-28 18:26:20 INFO  Execution'abd35ed0e22c9f54a08c4a5ed85dd7a7', 1624728859, 60504.861155, 427.825316, 114.0}
            2021-06-28 18:26:20 INFO  Execution'abd35ed0e22c9f54a08c4a5ed85dd7a7', 1624734774, 63119.504489, 465.399967, 115.0}
            2021-06-28 18:26:20 INFO  Execution'abd35ed0e22c9f54a08c4a5ed85dd7a7', 1624742294, 66475.252778, 497.885988, 115.0}
            2021-06-28 18:26:20 INFO  Execution'abd35ed0e22c9f54a08c4a5ed85dd7a7', 1624784360, 69696.506007, 374.251553, 119.0}
            2021-06-28 18:26:20 INFO  Execution'abd35ed0e22c9f54a08c4a5ed85dd7a7', 1624759640, 72752.972738, 631.650473, 143.0}
            2021-06-28 18:26:20 INFO  Execution'abd35ed0e22c9f54a08c4a5ed85dd7a7', 1624772776, 75899.839854, 576.333248, 141.0}
            2021-06-28 18:26:20 INFO  Execution'abd35ed0e22c9f54a08c4a5ed85dd7a7', 1624752324, 78726.5022, 505.308248, 146.0}
            2021-06-28 18:26:20 INFO  Execution'abd35ed0e22c9f54a08c4a5ed85dd7a7', 1624770650, 82031.626578, 559.762201, 183.0}
            2021-06-28 18:26:20 INFO  Execution'abd35ed0e22c9f54a08c4a5ed85dd7a7', 1624764420, 85338.09337, 396.332193, 173.0}
            2021-06-28 18:26:20 INFO  Execution'abd35ed0e22c9f54a08c4a5ed85dd7a7', 1624768460, 88480.158292, 526.736171, 207.0}
            2021-06-28 18:26:20 INFO  Execution'abd35ed0e22c9f54a08c4a5ed85dd7a7', 1624766282, 91203.0994, 81.744767, 204.0}
            2021-06-28 18:26:20 INFO  ExecutionManager:116 - JobMetrics{experimentId=6, jobId='9750a2d14be943a18687f3ac8b1dd0bd', config=30000.0, minDuration=422, avgDuration=1119, maxDuration=5904, minSize=536346, avgSize=6998755, maxSize=21534723, startTs=1624718818, stopTs=1624811505}
            jobId='9750a2d14be943a18687f3ac8b1dd0bd', timestamp=1624722288, avgThr=57162.491548, avgLat=683.537932, recTime=149.0}
            jobId='9750a2d14be943a18687f3ac8b1dd0bd', timestamp=1624728859, avgThr=60559.922233, avgLat=527.639635, recTime=136.0}
            jobId='9750a2d14be943a18687f3ac8b1dd0bd', timestamp=1624734774, avgThr=63360.696444, avgLat=366.702583, recTime=124.0}
            jobId='9750a2d14be943a18687f3ac8b1dd0bd', timestamp=1624742294, avgThr=66424.084667, avgLat=461.85245, recTime=141.0}
            jobId='9750a2d14be943a18687f3ac8b1dd0bd', timestamp=1624784360, avgThr=70143.759089, avgLat=465.30603, recTime=173.0}
            jobId='9750a2d14be943a18687f3ac8b1dd0bd', timestamp=1624759643, avgThr=72975.698062, avgLat=520.721379, recTime=175.0}
            jobId='9750a2d14be943a18687f3ac8b1dd0bd', timestamp=1624772776, avgThr=75920.93558, avgLat=568.234967, recTime=174.0}
            jobId='9750a2d14be943a18687f3ac8b1dd0bd', timestamp=1624752324, avgThr=78800.404981, avgLat=640.94142, recTime=168.0}
            jobId='9750a2d14be943a18687f3ac8b1dd0bd', timestamp=1624770650, avgThr=81961.254696, avgLat=502.169518, recTime=132.0}
            jobId='9750a2d14be943a18687f3ac8b1dd0bd', timestamp=1624764423, avgThr=85196.933622, avgLat=659.145042, recTime=315.0}
            jobId='9750a2d14be943a18687f3ac8b1dd0bd', timestamp=1624768460, avgThr=88387.710417, avgLat=860.194286, recTime=199.0}
            jobId='9750a2d14be943a18687f3ac8b1dd0bd', timestamp=1624766282, avgThr=91159.436122, avgLat=467.678389, recTime=236.0}
            2021-06-28 18:26:20 INFO  ExecutionManager:116 - JobMetrics{experimentId=6, jobId='3081547713d5a9bde12cfd00c2cdb8e4', config=60000.0, minDuration=495, avgDuration=1320, maxDuration=3805, minSize=710226, avgSize=8858519, maxSize=32440911, startTs=1624718818, stopTs=1624811505}
            jobId='3081547713d5a9bde12cfd00c2cdb8e4', timestamp=1624722288, avgThr=56968.001575, avgLat=574.381354, recTime=142.0}
            jobId='3081547713d5a9bde12cfd00c2cdb8e4', timestamp=1624728859, avgThr=60724.865359, avgLat=469.833223, recTime=141.0}
            jobId='3081547713d5a9bde12cfd00c2cdb8e4', timestamp=1624734774, avgThr=63474.823157, avgLat=777.269012, recTime=187.0}
            jobId='3081547713d5a9bde12cfd00c2cdb8e4', timestamp=1624742294, avgThr=66374.020288, avgLat=717.386512, recTime=195.0}
            jobId='3081547713d5a9bde12cfd00c2cdb8e4', timestamp=1624784360, avgThr=69876.073294, avgLat=507.746836, recTime=210.0}
            jobId='3081547713d5a9bde12cfd00c2cdb8e4', timestamp=1624759643, avgThr=72706.116075, avgLat=788.100939, recTime=203.0}
            jobId='3081547713d5a9bde12cfd00c2cdb8e4', timestamp=1624772776, avgThr=75821.005322, avgLat=495.951836, recTime=261.0}
            jobId='3081547713d5a9bde12cfd00c2cdb8e4', timestamp=1624752324, avgThr=78936.356671, avgLat=512.85652, recTime=203.0}
            jobId='3081547713d5a9bde12cfd00c2cdb8e4', timestamp=1624770650, avgThr=82082.34644, avgLat=546.773812, recTime=246.0}
            jobId='3081547713d5a9bde12cfd00c2cdb8e4', timestamp=1624764423, avgThr=85316.096858, avgLat=989.508281, recTime=268.0}
            jobId='3081547713d5a9bde12cfd00c2cdb8e4', timestamp=1624768460, avgThr=88524.066648, avgLat=922.333497, recTime=261.0}
            jobId='3081547713d5a9bde12cfd00c2cdb8e4', timestamp=1624766282, avgThr=91219.458673, avgLat=887.331453, recTime=295.0}
            2021-06-28 18:26:20 INFO  ExecutionManager:116 - JobMetrics{experimentId=6, jobId='f356fdcd64c59d779c955789a2165186', config=90000.0, minDuration=520, avgDuration=1383, maxDuration=3910, minSize=1545194, avgSize=9509242, maxSize=41388581, startTs=1624718818, stopTs=1624811505}
            jobId='f356fdcd64c59d779c955789a2165186', timestamp=1624722288, avgThr=56934.436612, avgLat=610.762375, recTime=1.0}
            jobId='f356fdcd64c59d779c955789a2165186', timestamp=1624728859, avgThr=60391.510379, avgLat=653.848331, recTime=209.0}
            jobId='f356fdcd64c59d779c955789a2165186', timestamp=1624734774, avgThr=63166.947041, avgLat=452.716188, recTime=194.0}
            jobId='f356fdcd64c59d779c955789a2165186', timestamp=1624742294, avgThr=66417.392037, avgLat=420.742375, recTime=1.0}
            jobId='f356fdcd64c59d779c955789a2165186', timestamp=1624784360, avgThr=69824.58693, avgLat=531.805839, recTime=198.0}
            jobId='f356fdcd64c59d779c955789a2165186', timestamp=1624759646, avgThr=72595.69107, avgLat=512.396337, recTime=1.0}
            jobId='f356fdcd64c59d779c955789a2165186', timestamp=1624772776, avgThr=75879.859568, avgLat=551.400598, recTime=192.0}
            jobId='f356fdcd64c59d779c955789a2165186', timestamp=1624752327, avgThr=78807.183185, avgLat=644.434427, recTime=223.0}
            jobId='f356fdcd64c59d779c955789a2165186', timestamp=1624770650, avgThr=81818.854679, avgLat=502.084942, recTime=252.0}
            jobId='f356fdcd64c59d779c955789a2165186', timestamp=1624764423, avgThr=85090.676802, avgLat=455.066611, recTime=291.0}
            jobId='f356fdcd64c59d779c955789a2165186', timestamp=1624768460, avgThr=88383.106479, avgLat=518.225706, recTime=328.0}
            jobId='f356fdcd64c59d779c955789a2165186', timestamp=1624766282, avgThr=91193.843184, avgLat=701.294327, recTime=362.0}
            2021-06-28 18:26:20 INFO  ExecutionManager:116 - JobMetrics{experimentId=6, jobId='2f167e8f5cf8204746e2234cc031c454', config=120000.0, minDuration=676, avgDuration=1551, maxDuration=7475, minSize=2838254, avgSize=11844451, maxSize=46101406, startTs=1624718818, stopTs=1624811505}
            jobId='2f167e8f5cf8204746e2234cc031c454', timestamp=1624722288, avgThr=57210.610823, avgLat=532.993713, recTime=1.0}
            jobId='2f167e8f5cf8204746e2234cc031c454', timestamp=1624728859, avgThr=60609.433078, avgLat=409.132674, recTime=208.0}
            jobId='2f167e8f5cf8204746e2234cc031c454', timestamp=1624734774, avgThr=63053.437974, avgLat=402.05608, recTime=1.0}
            jobId='2f167e8f5cf8204746e2234cc031c454', timestamp=1624742294, avgThr=66479.88447, avgLat=439.21157, recTime=1.0}
            jobId='2f167e8f5cf8204746e2234cc031c454', timestamp=1624784360, avgThr=69855.953632, avgLat=528.551179, recTime=232.0}
            jobId='2f167e8f5cf8204746e2234cc031c454', timestamp=1624759646, avgThr=72564.002179, avgLat=367.860216, recTime=292.0}
            jobId='2f167e8f5cf8204746e2234cc031c454', timestamp=1624772776, avgThr=75863.11156, avgLat=548.925532, recTime=1.0}
            jobId='2f167e8f5cf8204746e2234cc031c454', timestamp=1624752327, avgThr=78659.788331, avgLat=431.074444, recTime=1.0}
            jobId='2f167e8f5cf8204746e2234cc031c454', timestamp=1624770650, avgThr=81975.094957, avgLat=863.160249, recTime=1.0}
            jobId='2f167e8f5cf8204746e2234cc031c454', timestamp=1624764423, avgThr=85064.743988, avgLat=488.578729, recTime=373.0}
            jobId='2f167e8f5cf8204746e2234cc031c454', timestamp=1624768460, avgThr=88457.585625, avgLat=478.639568, recTime=1.0}
            jobId='2f167e8f5cf8204746e2234cc031c454', timestamp=1624766282, avgThr=91096.567424, avgLat=702.767384, recTime=384.0}*/

            /*List<Tuple2<String, List<Long>>> jobs = new ArrayList();
            jobs.add(Tuple2.apply("f356fdcd64c59d779c955789a2165186", Arrays.asList(1624722288L,1624742294L,1624759646L)));
            jobs.add(Tuple2.apply("2f167e8f5cf8204746e2234cc031c454", Arrays.asList(1624722288L, 1624734774L, 1624742294L, 1624772776L, 1624752327L, 1624770650L, 1624768460L)));

            int numOfCores = Runtime.getRuntime().availableProcessors();
            final ExecutorService executor = Executors.newFixedThreadPool(numOfCores);
            AtomicInteger counter = new AtomicInteger(0);
            final CountDownLatch latch = new CountDownLatch(12*5);

            LOG.info("Starting measure recovery times");
            long startTs = 1624718847;
            long stopTs = 1624794447;

            for (Tuple2<String, List<Long>> job : jobs) {

                TimeSeries thrTs = context.clientsManager.getThroughput(job._1, startTs, stopTs);
                TimeSeries lagTs = context.clientsManager.getConsumerLag(job._1, startTs, stopTs);

                for (Long timestamp : job._2) {

                    executor.submit(() -> {

                        try {
                            AnomalyDetector detector = new AnomalyDetector(Arrays.asList(thrTs, lagTs));
                            detector.fit(timestamp, 1000);
                            double recTime = detector.measure(timestamp);

                            LOG.info(job._1 + " " + timestamp + " " + recTime);
                            context.IOManager.updateRecTime(job._1, timestamp, recTime);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }
            }
            latch.await();*/


            /*context.IOManager.addFailureMetrics(6, "abd35ed0e22c9f54a08c4a5ed85dd7a7", 1624722285, 57123.342281, 438.153721);
            context.IOManager.addFailureMetrics(6, "abd35ed0e22c9f54a08c4a5ed85dd7a7", 1624728859, 60504.861155, 427.825316);
            context.IOManager.addFailureMetrics(6, "abd35ed0e22c9f54a08c4a5ed85dd7a7", 1624734774, 63119.504489, 465.399967);
            context.IOManager.addFailureMetrics(6, "abd35ed0e22c9f54a08c4a5ed85dd7a7", 1624742294, 66475.252778, 497.885988);
            context.IOManager.addFailureMetrics(6, "abd35ed0e22c9f54a08c4a5ed85dd7a7", 1624784360, 69696.506007, 374.251553);
            context.IOManager.addFailureMetrics(6, "abd35ed0e22c9f54a08c4a5ed85dd7a7", 1624759640, 72752.972738, 631.650473);
            context.IOManager.addFailureMetrics(6, "abd35ed0e22c9f54a08c4a5ed85dd7a7", 1624772776, 75899.839854, 576.333248);
            context.IOManager.addFailureMetrics(6, "abd35ed0e22c9f54a08c4a5ed85dd7a7", 1624752324, 78726.5022, 505.308248);
            context.IOManager.addFailureMetrics(6, "abd35ed0e22c9f54a08c4a5ed85dd7a7", 1624770650, 82031.626578, 559.762201);
            context.IOManager.addFailureMetrics(6, "abd35ed0e22c9f54a08c4a5ed85dd7a7", 1624764420, 85338.09337, 396.332193);
            context.IOManager.addFailureMetrics(6, "abd35ed0e22c9f54a08c4a5ed85dd7a7", 1624768460, 88480.158292, 526.736171);
            context.IOManager.addFailureMetrics(6, "abd35ed0e22c9f54a08c4a5ed85dd7a7", 1624766282, 91203.0994, 681.744767);

            context.IOManager.addFailureMetrics(6, "2f167e8f5cf8204746e2234cc031c454", 1624722288, 57210.610823, 532.993713);
            context.IOManager.addFailureMetrics(6, "2f167e8f5cf8204746e2234cc031c454", 1624728859, 60609.433078, 409.132674);
            context.IOManager.addFailureMetrics(6, "2f167e8f5cf8204746e2234cc031c454", 1624734774, 63053.437974, 402.05608);
            context.IOManager.addFailureMetrics(6, "2f167e8f5cf8204746e2234cc031c454", 1624742294, 66479.88447, 439.21157);
            context.IOManager.addFailureMetrics(6, "2f167e8f5cf8204746e2234cc031c454", 1624784360, 69855.953632, 528.551179);
            context.IOManager.addFailureMetrics(6, "2f167e8f5cf8204746e2234cc031c454", 1624759646, 72564.002179, 367.860216);
            context.IOManager.addFailureMetrics(6, "2f167e8f5cf8204746e2234cc031c454", 1624772776, 75863.11156, 548.925532);
            context.IOManager.addFailureMetrics(6, "2f167e8f5cf8204746e2234cc031c454", 1624752327, 78659.788331, 431.074444);
            context.IOManager.addFailureMetrics(6, "2f167e8f5cf8204746e2234cc031c454", 1624770650, 81975.094957, 863.160249);
            context.IOManager.addFailureMetrics(6, "2f167e8f5cf8204746e2234cc031c454", 1624764423, 85064.743988, 488.578729);
            context.IOManager.addFailureMetrics(6, "2f167e8f5cf8204746e2234cc031c454", 1624768460, 88457.585625, 478.639568);
            context.IOManager.addFailureMetrics(6, "2f167e8f5cf8204746e2234cc031c454", 1624766282, 91096.567424, 702.767384);

            context.IOManager.addFailureMetrics(6, "9750a2d14be943a18687f3ac8b1dd0bd", 1624722288, 57162.491548, 683.537932);
            context.IOManager.addFailureMetrics(6, "9750a2d14be943a18687f3ac8b1dd0bd", 1624728859, 60559.922233, 527.639635);
            context.IOManager.addFailureMetrics(6, "9750a2d14be943a18687f3ac8b1dd0bd", 1624734774, 63360.696444, 366.702583);
            context.IOManager.addFailureMetrics(6, "9750a2d14be943a18687f3ac8b1dd0bd", 1624742294, 66424.084667, 461.85245);
            context.IOManager.addFailureMetrics(6, "9750a2d14be943a18687f3ac8b1dd0bd", 1624784360, 70143.759089, 465.30603);
            context.IOManager.addFailureMetrics(6, "9750a2d14be943a18687f3ac8b1dd0bd", 1624759643, 72975.698062, 520.721379);
            context.IOManager.addFailureMetrics(6, "9750a2d14be943a18687f3ac8b1dd0bd", 1624772776, 75920.93558, 568.234967);
            context.IOManager.addFailureMetrics(6, "9750a2d14be943a18687f3ac8b1dd0bd", 1624752324, 78800.404981, 640.94142);
            context.IOManager.addFailureMetrics(6, "9750a2d14be943a18687f3ac8b1dd0bd", 1624770650, 81961.254696, 502.169518);
            context.IOManager.addFailureMetrics(6, "9750a2d14be943a18687f3ac8b1dd0bd", 1624764423, 85196.933622, 659.145042);
            context.IOManager.addFailureMetrics(6, "9750a2d14be943a18687f3ac8b1dd0bd", 1624768460, 88387.710417, 860.194286);
            context.IOManager.addFailureMetrics(6, "9750a2d14be943a18687f3ac8b1dd0bd", 1624766282, 91159.436122, 467.678389);

            context.IOManager.addFailureMetrics(6, "3081547713d5a9bde12cfd00c2cdb8e4", 1624722288, 56968.001575, 574.381354);
            context.IOManager.addFailureMetrics(6, "3081547713d5a9bde12cfd00c2cdb8e4", 1624728859, 60724.865359, 469.833223);
            context.IOManager.addFailureMetrics(6, "3081547713d5a9bde12cfd00c2cdb8e4", 1624734774, 63474.823157, 777.269012);
            context.IOManager.addFailureMetrics(6, "3081547713d5a9bde12cfd00c2cdb8e4", 1624742294, 66374.020288, 717.386512);
            context.IOManager.addFailureMetrics(6, "3081547713d5a9bde12cfd00c2cdb8e4", 1624784360, 69876.073294, 507.746836);
            context.IOManager.addFailureMetrics(6, "3081547713d5a9bde12cfd00c2cdb8e4", 1624759643, 72706.116075, 788.100939);
            context.IOManager.addFailureMetrics(6, "3081547713d5a9bde12cfd00c2cdb8e4", 1624772776, 75821.005322, 495.951836);
            context.IOManager.addFailureMetrics(6, "3081547713d5a9bde12cfd00c2cdb8e4", 1624752324, 78936.356671, 512.85652);
            context.IOManager.addFailureMetrics(6, "3081547713d5a9bde12cfd00c2cdb8e4", 1624770650, 82082.34644, 546.773812);
            context.IOManager.addFailureMetrics(6, "3081547713d5a9bde12cfd00c2cdb8e4", 1624764423, 85316.096858, 989.508281);
            context.IOManager.addFailureMetrics(6, "3081547713d5a9bde12cfd00c2cdb8e4", 1624768460, 88524.066648, 922.333497);
            context.IOManager.addFailureMetrics(6, "3081547713d5a9bde12cfd00c2cdb8e4", 1624766282, 91219.458673, 887.331453);

            context.IOManager.addFailureMetrics(6, "f356fdcd64c59d779c955789a2165186", 1624722288, 56934.436612, 610.762375);
            context.IOManager.addFailureMetrics(6, "f356fdcd64c59d779c955789a2165186", 1624728859, 60391.510379, 653.848331);
            context.IOManager.addFailureMetrics(6, "f356fdcd64c59d779c955789a2165186", 1624734774, 63166.947041, 452.716188);
            context.IOManager.addFailureMetrics(6, "f356fdcd64c59d779c955789a2165186", 1624742294, 66417.392037, 420.742375);
            context.IOManager.addFailureMetrics(6, "f356fdcd64c59d779c955789a2165186", 1624784360, 69824.58693, 531.805839);
            context.IOManager.addFailureMetrics(6, "f356fdcd64c59d779c955789a2165186", 1624759646, 72595.69107, 512.396337);
            context.IOManager.addFailureMetrics(6, "f356fdcd64c59d779c955789a2165186", 1624772776, 75879.859568, 551.400598);
            context.IOManager.addFailureMetrics(6, "f356fdcd64c59d779c955789a2165186", 1624752327, 78807.183185, 644.434427);
            context.IOManager.addFailureMetrics(6, "f356fdcd64c59d779c955789a2165186", 1624770650, 81818.854679, 502.084942);
            context.IOManager.addFailureMetrics(6, "f356fdcd64c59d779c955789a2165186", 1624764423, 85090.676802, 455.066611);
            context.IOManager.addFailureMetrics(6, "f356fdcd64c59d779c955789a2165186", 1624768460, 88383.106479, 518.225706);
            context.IOManager.addFailureMetrics(6, "f356fdcd64c59d779c955789a2165186", 1624766282, 91193.843184, 701.294327);*/

            //long startTs = 1624966883;
            //long stopTs = 1625053189;
            //long startTs = 1624981604;
            //long stopTs = 1625067404;
            /*int experimentId = 7;

            String operatorId = "ddb598ad156ed281023ba4eebbe487e3";
            for (JobMetrics jobMetrics : context.IOManager.fetchJobMetricsList(experimentId)) {

                LOG.info(jobMetrics);

                TimeSeries ts = context.clientsManager.getLatency(jobMetrics.jobId, operatorId, jobMetrics.startTs, jobMetrics.stopTs);
                TimeSeries.toCSV("iot_baseline_7_" + jobMetrics.config + ".csv", ts, "timestamp|latency", "|");

                for (FailureMetrics failureMetrics : context.IOManager.fetchFailureMetricsList(experimentId, jobMetrics.jobId)) {

                    LOG.info(failureMetrics);
                }
            }*/

            /*int numOfCores = Runtime.getRuntime().availableProcessors();
            final ExecutorService executor = Executors.newFixedThreadPool(numOfCores);
            AtomicInteger counter = new AtomicInteger(0);
            final CountDownLatch latch = new CountDownLatch(12*5);

            LOG.info("Starting measure recovery times");

            for (JobMetrics jobMetrics : context.IOManager.fetchJobMetricsList(experimentId)) {

                LOG.info(jobMetrics);

                TimeSeries thrTs = context.clientsManager.getThroughput(jobMetrics.jobId, startTs, stopTs);
                TimeSeries lagTs = context.clientsManager.getConsumerLag(jobMetrics.jobId, startTs, stopTs);

                for (FailureMetrics failureMetrics : context.IOManager.fetchFailureMetricsList(experimentId, jobMetrics.jobId)) {

                    LOG.info(failureMetrics);
                    executor.submit(() -> {

                        try {
                            AnomalyDetector detector = new AnomalyDetector(Arrays.asList(thrTs, lagTs));
                            detector.fit(failureMetrics.timestamp, 1000);
                            double recTime = detector.measure(failureMetrics.timestamp);

                            LOG.info(jobMetrics.jobId + " " + failureMetrics.timestamp + " " + recTime);
                            context.IOManager.updateRecTime(jobMetrics.jobId, failureMetrics.timestamp, recTime);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                        finally {
                            latch.countDown();
                        }
                    });
                }
            }
            latch.await();*/

            /*long stopTs = Instant.now().getEpochSecond();
            long startTs = stopTs - 3600;
            TimeSeries ts = context.clientsManager.getMsgInSec("iot-vehicles-events", 60, startTs, stopTs);
            List<Double> filteredList = Arrays.stream(ts.values()).filter(c -> !Double.isNaN(c)).boxed().collect(Collectors.toList());
            List<Double> filteredSubList = filteredList.subList(filteredList.size() % 60, filteredList.size());

            int count = 0;
            double total = 0;
            double[] dataPoints = new double[filteredList.size() / 60];
            for (int i = 0; i < filteredSubList.size(); i++) {

                total += filteredSubList.get(i);
                count++;

                if (count == 60) {

                    dataPoints[i / 60] = total / count;
                    total = 0;
                    count = 0;

                }
            }*/

            LOG.info(context.clientsManager.flink.saveAndStopJob("c2afb41bbdc4ac132dafea10c2421b9a", false, "/savepoints"));

            //Iterator<Double> it = filteredSubList.iterator();
            //List<Double> dataPoints = new ArrayList<>();
            //int count = 0;
            //double total = 0;
            //while (it.hasNext()) {
                //if (count == 60) {
                    //dataPoints.add(total / count);
                    //count = 0;
                    //total = 0;
                //}
                //total += it.next();
                //count++;
            //}
            //LOG.info(Arrays.toString(dataPoints.toArray()));

            //LOG.info(context.clientsManager.getUptime("798a303c87e3dd3d7a70ba5fb03ef69e"));

            return STOP;
        }
    },
    RECORD {

        public ExecutionManager runStage(Context context) {

            // saves events from kafka consumer topic to database for a user defined time
            if (context.doRecord) {

                context.IOManager.recordKafkaToDatabase(context.consumerTopic, context.timeLimit, 10000);
                context.IOManager.extractFullWorkload();
            }
            context.IOManager.extractFailureScenario(0.5f);
            for (Tuple3<Integer, Long, Integer> current : context.IOManager.getFailureScenario()) {

                LOG.info(current._1() + " " + current._2() + " " + current._3());
            }
            return DEPLOY;
        }
    },
    DEPLOY {

        public ExecutionManager runStage(Context context) throws Exception {

            // deploy parallel profiling jobs
            for (Job job : context.experiment.jobs) {

                job.setJobId(context.clientsManager.startJob(job.getProgramArgs()));
                job.setOperatorIds(context.clientsManager.getOperatorIds(job.getJobId()));
                job.setSinkId(context.clientsManager.getSinkOperatorId(job.getJobId(), context.sinkRegex));
            }

            return REGISTER;
        }
    },
    REGISTER {

        public ExecutionManager runStage(Context context) throws Exception {

            context.IOManager.initFailureMetrics(context.experimentId, true);

            // register points for failure injection with counter manager
            context.IOManager.getFailureScenario().forEach(point -> {

                context.IOManager.registerListener(new Listener(point._1(), () -> {

                    for (Job job : context.experiment.jobs) {

                        try {
                            // wait until close to the end of checkpoint interval and then inject failure
                            while (true) {

                                long currTs = context.clientsManager.getLatestTs(job.getJobId());
                                long chkLast = context.clientsManager.getLastCheckpoint(job.getJobId());
                                // find time to point which is 3 seconds from when next checkpoint starts
                                long target = (job.config / 1000) - (currTs - chkLast) - context.chkTolerance;
                                if (target > 0) {
                                    context.executor.schedule(() -> {
                                        try {
                                            context.clientsManager.injectFailure(job.getJobId(), job.getSinkId());
                                            long startTs = currTs - context.averagingWindow;
                                            double avgThr = context.clientsManager.getThroughput(job.getJobId(), startTs, currTs).average();
                                            double avgLat = context.clientsManager.getLatency(job.getJobId(), job.getSinkId(), startTs, currTs).average();
                                            context.IOManager.addFailureMetrics(context.experimentId, job.getJobId(), currTs, avgThr, avgLat);
                                            for (FailureMetrics failureMetrics : context.IOManager.fetchFailureMetricsList(context.experimentId, job.getJobId())) {

                                                LOG.info(failureMetrics);
                                            }
                                            LOG.info("Finishing inject failure into job " + job.getJobId());
                                        }
                                        catch (Exception e) {

                                            e.printStackTrace();
                                            LOG.error("Failed to inject scheduled failure with message " + e.fillInStackTrace());
                                        }
                                    }, target, TimeUnit.SECONDS);
                                    break;
                                }
                                new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS);
                            }
                        }
                        catch (Exception e) {

                            LOG.error("Failed to inject failure with message " + e.fillInStackTrace());
                        }
                    }
                }));
            });
            return REPLAY;
        }
    },
    REPLAY {

        public ExecutionManager runStage(Context context) {

            // record start time of experiment
            context.experiment.setStartTs(Instant.now().getEpochSecond());

            BlockingQueue<List<String>> queue = new ArrayBlockingQueue<>(60);
            // TODO for baselines, can remove
            if (context.doReplayAll) {

                int startIndex = 0;
                int stopIndex = context.IOManager.getFullWorkload().size();
                AtomicBoolean isDone = new AtomicBoolean(false);
                CompletableFuture
                    .runAsync(context.IOManager.databaseToQueue(startIndex, stopIndex, queue))
                    .thenRun(() -> isDone.set(true));
                // wait till queue has items in it
                try { while (queue.isEmpty()) new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS); }
                catch(InterruptedException ex) { LOG.error(ex); }
                //
                context.IOManager.queueToKafka(startIndex, queue, context.experiment.consumerTopic, isDone).run();
            }
            else {

                context.IOManager.getFailureScenario().forEach(point -> {
                    // select range before and after failure point based on minInterval but smaller
                    int startIndex = point._1() - context.minInterval * 2/3;
                    int stopIndex = point._1() + context.minInterval * 2/3;

                    AtomicBoolean isDone = new AtomicBoolean(false);
                    CompletableFuture
                        .runAsync(context.IOManager.databaseToQueue(startIndex, stopIndex, queue))
                        .thenRun(() -> isDone.set(true));
                    // wait till queue has items in it
                    try { while (queue.isEmpty()) new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS); }
                    catch(InterruptedException ex) { LOG.error(ex); }
                    //
                    context.IOManager.queueToKafka(startIndex, queue, context.experiment.consumerTopic, isDone).run();
                });
            }
            // record end time of experiment
            context.experiment.setStopTs(Instant.now().getEpochSecond());

            return DELETE;
        }
    },
    DELETE {

        public ExecutionManager runStage(Context context) throws Exception {

            context.IOManager.initJobMetrics(context.experimentId, true);

            for (Job job : context.experiment.jobs) {

                // save checkpoint metrics
                Checkpoints checkpoints = context.clientsManager.flink.getCheckpoints(job.getJobId());
                context.IOManager.addJobMetrics(
                    context.experimentId,
                    job.getJobId(),
                    job.config,
                    checkpoints.summary.endToEndDuration.min,
                    checkpoints.summary.endToEndDuration.avg,
                    checkpoints.summary.endToEndDuration.max,
                    checkpoints.summary.stateSize.min,
                    checkpoints.summary.stateSize.avg,
                    checkpoints.summary.stateSize.max,
                    context.experiment.getStartTs(),
                    context.experiment.getStopTs());

                // Stop experimental job
                context.clientsManager.flink.stopJob(job.getJobId());
            }
            return MEASURE;
        }
    },
    MEASURE {

        public ExecutionManager runStage(Context context) throws Exception {

            int numOfCores = Runtime.getRuntime().availableProcessors();
            final ExecutorService executor = Executors.newFixedThreadPool(numOfCores);
            int total = context.numOfConfigs * context.numFailures;
            AtomicInteger counter = new AtomicInteger(0);
            final CountDownLatch latch = new CountDownLatch(total);

            LOG.info("Starting measure recovery times");
            for (JobMetrics jobMetrics : context.IOManager.fetchJobMetricsList(context.experimentId)) {

                TimeSeries thrTs = context.clientsManager.getThroughput(jobMetrics.jobId, jobMetrics.startTs, jobMetrics.stopTs);
                TimeSeries lagTs = context.clientsManager.getConsumerLag(jobMetrics.jobId, jobMetrics.startTs, jobMetrics.stopTs);

                for (FailureMetrics failureMetrics : context.IOManager.fetchFailureMetricsList(context.experimentId, jobMetrics.jobId)) {

                    executor.submit(() -> {

                        AnomalyDetector detector = new AnomalyDetector(Arrays.asList(thrTs, lagTs));
                        detector.fit(failureMetrics.timestamp, 1000);
                        double recTime = detector.measure(failureMetrics.timestamp);

                        LOG.info(failureMetrics.jobId + " " + failureMetrics.timestamp + " " + recTime);
                        context.IOManager.updateRecTime(failureMetrics.jobId, failureMetrics.timestamp, recTime);
                        LOG.info(counter.incrementAndGet() + "/" + total + " completed");
                        latch.countDown();
                    });
                }
            }
            latch.await();
            executor.shutdown();
            LOG.info("Finished measure recovery times");

            return STOP;
        }
    },
    MODEL {

        public ExecutionManager runStage(Context context) {

            /*Map<Double, List<Tuple3<Double, Double, Double>>> perfMap = new TreeMap<>();
            Map<Double, List<Tuple3<Double, Double, Double>>> availMap= new TreeMap<>();
            for (int i = 1; i <= 5; i++) {

                for (JobMetrics jobMetrics : context.IOManager.fetchJobMetricsList(i)) {

                    perfMap.putIfAbsent(jobMetrics.config, new ArrayList<>());
                    availMap.putIfAbsent(jobMetrics.config, new ArrayList<>());

                    for (FailureMetrics failureMetrics : context.IOManager.fetchFailureMetricsList(i, jobMetrics.jobId)) {

                        perfMap.get(jobMetrics.config).add(Tuple3.apply(failureMetrics.avgThr, jobMetrics.config, failureMetrics.avgLat));
                        availMap.get(jobMetrics.config).add(Tuple3.apply(failureMetrics.avgThr, jobMetrics.config, failureMetrics.recTime));
                    }
                }
            }
            LOG.info(perfMap);
            LOG.info(availMap);

            List<Tuple3<Double, Double, Double>> perfAvgList = new ArrayList<>();
            for (Map.Entry<Double, List<Tuple3<Double, Double, Double>>> entry : perfMap.entrySet()) {

                int countThr = 0;
                double totalThr = 0;
                int countLat = 0;
                double totalLat = 0;
                for (int i = 0; i < entry.getValue().size(); i++) {

                    totalThr += entry.getValue().get(i)._1();
                    countThr++;
                    totalLat += entry.getValue().get(i)._3();
                    countLat++;
                }
                perfAvgList.add(new Tuple3<>(totalThr/countThr, entry.getKey(), totalLat/countLat));
            }
            LOG.info(Arrays.toString(perfAvgList.toArray()));

            List<Tuple3<Double, Double, Double>> availAvgList = new ArrayList<>();
            for (Map.Entry<Double, List<Tuple3<Double, Double, Double>>> entry : availMap.entrySet()) {

                int countThr = 0;
                double totalThr = 0;
                int countRecTime = 0;
                double totalRecTime = 0;
                for (int i = 0; i < entry.getValue().size(); i++) {

                    if (entry.getValue().get(i)._3() > 55) {

                        totalThr += entry.getValue().get(i)._1();
                        countThr++;
                        countRecTime += entry.getValue().get(i)._3();
                        totalRecTime++;
                    }

                }
                availAvgList.add(new Tuple3<>(totalThr/countThr, entry.getKey(), totalRecTime/countRecTime));
            }
            LOG.info(Arrays.toString(availAvgList.toArray()));

            LOG.info("-PERFORMANCE -----------------------------------------------------------------------");
            context.performance.fit(Tuple3.apply("thr", "conf", "lat"), perfAvgList, "lat");
            //LOG.info(context.performance.getModel());
            LOG.info(context.performance.predict("thr", 15000D, "conf", 30000D));

            LOG.info("-AVAILABILITY -----------------------------------------------------------------------");
            context.availability.fit(Tuple3.apply("thr", "conf", "recTime"), availAvgList, "recTime");
            //LOG.info(context.availability.getModel());
            LOG.info(context.availability.predict("thr", 15000D, "conf", 30000D));*/


            for (int i = 1; i <= 5; i++) {

                List<Tuple3<Double, Double, Double>> perfList = new ArrayList<>();
                List<Tuple3<Double, Double, Double>> availList = new ArrayList<>();
                for (JobMetrics jobMetrics : context.IOManager.fetchJobMetricsList(i)) {

                    for (FailureMetrics failureMetrics : context.IOManager.fetchFailureMetricsList(jobMetrics.experimentId, jobMetrics.jobId)) {

                        perfList.add(new Tuple3<>(failureMetrics.avgThr, jobMetrics.config, failureMetrics.avgLat));
                        if (failureMetrics.recTime > 55) availList.add(new Tuple3<>(failureMetrics.avgThr, jobMetrics.config, failureMetrics.recTime));
                    }
                }
                LOG.info("-PERFORMANCE " + i + " -----------------------------------------------------------------------");
                context.performance.fit(Tuple3.apply("thr", "conf", "lat"), perfList, "lat");
                LOG.info(Arrays.toString(context.performance.predict("thr", 15000D, "conf", 30000D)));

                LOG.info("-AVAILABILITY " + i + " -----------------------------------------------------------------------");
                context.availability.fit(Tuple3.apply("thr", "conf", "recTime"), availList, "recTime");
                LOG.info(Arrays.toString(context.availability.predict("thr", 15000D, "conf", 30000D)));
            }

            //-----------------------------------------------------------------------------------------

            Map<Double, List<List<Double>>> perfMap = new TreeMap<>();
            Map<Double, List<List<Double>>> availMap= new TreeMap<>();
            for (int i = 1; i <= 5; i++) {

                for (JobMetrics jobMetrics : context.IOManager.fetchJobMetricsList(i)) {

                    perfMap.putIfAbsent(jobMetrics.config, new ArrayList<>());
                    availMap.putIfAbsent(jobMetrics.config, new ArrayList<>());

                    for (FailureMetrics failureMetrics : context.IOManager.fetchFailureMetricsList(i, jobMetrics.jobId)) {

                        perfMap.get(jobMetrics.config).add(Arrays.asList(failureMetrics.avgThr, jobMetrics.config, failureMetrics.avgLat));
                        availMap.get(jobMetrics.config).add(Arrays.asList(failureMetrics.avgThr, jobMetrics.config, failureMetrics.recTime));
                    }
                }
            }

            int counter1 = 0, counter2 = 0;
            double [][] perfArr = new double[context.numOfConfigs * context.numFailures][];
            double [][] availArr = new double[context.numOfConfigs * context.numFailures - 7][];
            for (Map.Entry<Double, List<List<Double>>> entry : perfMap.entrySet()) {

                double key = entry.getKey();
                final int chunkSize = 5;

                final AtomicInteger count = new AtomicInteger(0);
                List<List<Double>> perfs = entry.getValue();
                perfs.sort(Comparator.comparing(o -> o.get(0)));
                final Collection<List<List<Double>>> perfRes =
                    perfs.stream()
                        .collect(Collectors.groupingBy(it -> count.getAndIncrement() / chunkSize))
                        .values();

                count.set(0);
                List<List<Double>> avails = availMap.get(key);
                avails.sort(Comparator.comparing(o -> o.get(0)));
                final Collection<List<List<Double>>> availRes =
                    avails.stream()
                        .collect(Collectors.groupingBy(it -> count.getAndIncrement() / chunkSize))
                        .values();

                Iterator<List<List<Double>>> it1 = perfRes.iterator();
                Iterator<List<List<Double>>> it2 = availRes.iterator();
                while (it1.hasNext()) {

                    List<List<Double>> currPerf = it1.next();
                    perfArr[counter1] = ArrayUtils.toPrimitive(currPerf.get(2).toArray(new Double[0]));
                    counter1++;
                    List<List<Double>> currAvail = it2.next();
                    double[] temp = ArrayUtils.toPrimitive(currAvail.get(2).toArray(new Double[0]));
                    if (temp[2] > 55) {

                        availArr[counter2] = temp;
                        counter2++;
                    }
                }
            }
            for (int i = 0; i < perfArr.length; i++) {

                System.out.println(Arrays.toString(perfArr[i]));
            }
            for (int i = 0; i < availArr.length; i++) {

                System.out.println(Arrays.toString(availArr[i]));
            }

            LOG.info("-PERFORMANCE -----------------------------------------------------------------------");
            context.performance.fit(Tuple3.apply("thr", "conf", "lat"), perfArr, "lat");
            //LOG.info(context.performance.getModel());
            LOG.info(Arrays.toString(context.performance.predict("thr", 15000D, "conf", 30000D)));

            LOG.info("-AVAILABILITY -----------------------------------------------------------------------");
            context.availability.fit(Tuple3.apply("thr", "conf", "recTime"), availArr, "recTime");
            //LOG.info(context.availability.getModel());
            LOG.info(Arrays.toString(context.availability.predict("thr", 15000D, "conf", 30000D)));

            //LOG.info(Arrays.deepToString(perfArr));
            //LOG.info(Arrays.deepToString(availArr));

            /*double [][] perfArr = new double[perfList.size()][];
            double [][] availArr = new double[availList.size()][];
            for (int i = 0; i < perfList.size(); i++) {

                perfArr[i] = ArrayUtils.toPrimitive(perfList.get(i).toArray(new Double[0]));
                availArr[i] = ArrayUtils.toPrimitive(availList.get(i).toArray(new Double[0]));
            }
            LOG.info(Arrays.deepToString(perfArr));
            LOG.info(Arrays.deepToString(availArr));*/

            /*DataFrame dataDf = DataFrame.of(perfArr, "thr", "conf", "lat");
            LOG.info(dataDf);
            LinearModel model = OLS.fit(Formula.lhs("lat"), dataDf);
            LOG.info(model);
            LOG.info(Arrays.toString(model.coefficients()));

            double[][] tester = new double[1][];
            tester[0] = new double[]{15000D, 50000D};
            DataFrame test = DataFrame.of(tester, "thr", "conf");
            LOG.info(Arrays.toString(model.predict(test)));
            LOG.info(model.predict(new double[]{15000D, 50000D, 0D}));

            DataFrame dataDf2 = DataFrame.of(availArr, "recTime", "conf", "thr");
            LOG.info(dataDf2);
            LinearModel model2 = OLS.fit(Formula.lhs("recTime"), dataDf2);
            LOG.info(model2);
            LOG.info(Arrays.toString(model2.coefficients()));
            LOG.info(model2.predict(new double[]{0D, 50000D, 15000D}));*/

            // fit models for performance and availability
            //context.performance.fit(avgLats_apache, confThr_apache);
            //context.availability.fit(durHats, Arrays.asList(configs, avgThrs));
            //LOG.info(context.performance.predict(Arrays.asList(50000D, 30000D)));


            // Log R^2 and P-values for models
            //LOG.info(context.performance.calculateRSquared());
            //LOG.info(Arrays.toString(context.performance.calculatePValues().toArray()));
            //LOG.info(context.availability.calculateRSquared());
            //LOG.info(Arrays.toString(context.availability.calculatePValues().toArray()));

            return STOP;
        }
    },
    OPTIMIZE {

        public ExecutionManager runStage(Context context) throws Exception {

            context.targetJob.setSinkId(context.clientsManager.getSinkOperatorId(context.jobId, context.sinkRegex));

            final StopWatch stopWatch = new StopWatch();
            while (true) {

                try {
                    // default
                    int optMultiplier = 1;
                    // ensure 
                    long uptime = context.clientsManager.getUptime(context.jobId);
                    if (uptime < context.minUpTime) {

                        new CountDownLatch(1).await((context.minUpTime - uptime), TimeUnit.SECONDS);
                        continue;
                    }

                    long stopTs = context.clientsManager.getLatestTs(context.targetJob.getJobId());
                    long startTs = stopTs - context.averagingWindow;

                    // fetch metrics and predict recovery time based on current throughput and checkpoint interval
                    double avgLat = context.clientsManager.getLatency(context.jobId, context.targetJob.getSinkId(), startTs, stopTs).average();
                    double avgThr = context.clientsManager.getThroughput(context.jobId, startTs, stopTs).average();
                    double recTime = context.availability.predict("thr", avgThr, "conf", context.targetJob.config)[1];

                    // TODO how far do we want to look back?
                    int windowMultiplier = 5;

                    // evaluate metrics based on constraints
                    if ((context.avgLatConst < avgLat  && context.recTimeConst >= recTime) ||
                        (context.avgLatConst >= avgLat && context.recTimeConst < recTime)) {

                        // Find range of valid checkpoint intervals
                        int size = (context.maxConfigVal - context.minConfigVal) / 1000;
                        double[] chkIntArr = new double[size];
                        for (int i = 0; i < size; i++) {

                            chkIntArr[i] = context.minConfigVal + (i * 1000);
                        }

                        // fetch average of workload per minute for the last hour
                        TimeSeries ts = context.clientsManager.getMsgInSec(context.consumerTopic, 60, stopTs - 3600, stopTs);
                        List<Double> filteredList = Arrays.stream(ts.values()).filter(c -> !Double.isNaN(c)).boxed().collect(Collectors.toList());
                        List<Double> filteredSubList = filteredList.subList(filteredList.size() % 60, filteredList.size());
                        int count = 0;
                        double total = 0;
                        double[] dataPoints = new double[filteredList.size() / 60];
                        for (int i = 0; i < filteredSubList.size(); i++) {

                            total += filteredSubList.get(i);
                            count++;

                            if (count == 60) {

                                dataPoints[i / 60] = total / count;
                                total = 0;
                                count = 0;

                            }
                        }
                        double lastDataPoint = dataPoints[dataPoints.length - 1];
                        // simple estimate of how throughput will evolve
                        double[] predThrs = context.forecast.fit(dataPoints).predict((int) Math.ceil(context.optInterval / 60));
                        // derive recommendation: is throughput evolving rapidly?
                        double sumOffPointWiseDifferences = ForecastModel.computeDifferences(lastDataPoint, predThrs);
                        boolean isUrgent = sumOffPointWiseDifferences < -0.1 * lastDataPoint;

                        if ((context.avgLatConst * context.maxViolation) < avgLat ||
                            (context.recTimeConst * context.maxViolation) < recTime || isUrgent){
                            // perform optimization, get best new checkpoint interval
                            double checkpointInterval = Optimization.getOptimalCheckpointInterval(context, avgThr, chkIntArr);
                            // TODO do something with the calculated new checkpoint interval
                            // we need to increase the multiplier, as we will have to wait longer
                            optMultiplier = 2;
                        }

                    }
                    else if (context.recTimeConst < recTime && context.avgLatConst < avgLat) {

                        LOG.warn(String.format(
                            "Unable to optimize, %s < %f and %d < %f",
                            context.avgLatConst, avgLat, context.recTimeConst, recTime));
                    }
                    else {

                        LOG.info("No violation");
                    }

                    // wait until next interval is reached
                    stopWatch.start();
                    long current = stopWatch.getTime(TimeUnit.SECONDS);
                    while (current < context.optInterval * optMultiplier) {

                        current = stopWatch.getTime(TimeUnit.SECONDS);
                        new CountDownLatch(1).await(100, TimeUnit.MILLISECONDS);
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

        public ExecutionManager runStage(Context context) throws Exception {

            context.close();
            return this;
        }
    };

    private static final Logger LOG = Logger.getLogger(ExecutionManager.class);

    public static void start() throws Exception {

        LOG.info("START");
        ExecutionManager.START.run(ExecutionManager.class, Context.get);
    }
}
