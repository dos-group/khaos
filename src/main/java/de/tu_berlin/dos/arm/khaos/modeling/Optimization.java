package de.tu_berlin.dos.arm.khaos.modeling;

import de.tu_berlin.dos.arm.khaos.core.Context;
import org.apache.log4j.Logger;

import java.util.Arrays;

public class Optimization {

    public enum OptType {

        LATENCY,
        RECTIME;

        OptType() { }
    }

    private static final Logger LOG = Logger.getLogger(Optimization.class);

    private Optimization() {}

    /*public static double optimize(OptType type, Context context, double currentThr, double[] chkIntArr) throws Exception {

        double[] predLatArr =
            Arrays.stream(chkIntArr)
                .map(i -> context.performance.predict("thr", currentThr, "conf", i)[0])
                .toArray();
        double[] predRecArr =
            Arrays.stream(chkIntArr)
                .map(i -> context.availability.predict("thr", currentThr, "conf", i)[0])
                .toArray();

        double bestChkInt = -1;
        int index = 0;
        switch (type) {

            case LATENCY: {
                //double tarLatency = context.avgLatConst * 0.9;
                double predLat = context.performance.predict("thr", currentThr, "conf", context.targetJob.getConfig())[0];
                double tarLatency = predLat * 0.9;
                double distance = Math.abs(predLatArr[0] - tarLatency);
                for (int i = 1; i < predLatArr.length; i++) {

                    double curr = Math.abs(predLatArr[i] - tarLatency);

                    if (curr < distance) {

                        LOG.info("predLat: " + predLatArr[i] + ", distance" + curr + ", chpInt: " + chkIntArr[index]);
                        index = i;
                        distance = curr;
                    }
                }
                LOG.info("Distance to target Latency: (" + chkIntArr[index] + ", " + predLatArr[index] + " (" + distance + "))");
                if (context.recTimeConst < predRecArr[index])
                    throw new IllegalStateException("Unable to find good checkpoint");
                bestChkInt = chkIntArr[index];
                break;
            }
            case RECTIME: {

                double tarRecTime = context.recTimeConst * 0.9;
                double distance = Math.abs(predRecArr[0] - tarRecTime);
                for (int i = 1; i < predRecArr.length; i++) {

                    double curr = Math.abs(predRecArr[i] - tarRecTime);
                    if (curr < distance) {

                        index = i;
                        distance = curr;
                        LOG.info("predRec: " + predRecArr[i] + ", distance" + curr + ", chpInt: " + chkIntArr[index]);
                    }
                }
                LOG.info("Distance to target RecTime: (" + chkIntArr[index] + ", " + predRecArr[index] + " (" + distance + "))");
                if (context.avgLatConst < predLatArr[index])
                    throw new IllegalStateException("Unable to find good checkpoint");
                bestChkInt = chkIntArr[index];
                break;
            }
        }
        return bestChkInt;
    }*/

    public static double optimize(String type, Context context, double currThr, double[] chkIntArr, double weight) {

        double[] predLatArr = Arrays.stream(chkIntArr)
                .map(i -> context.performance.predict("thr", currThr, "conf", i)[0])
                .toArray();

        double[] predRecArr = Arrays.stream(chkIntArr)
                .map(i -> context.availability.predict("thr", currThr, "conf", i)[0])
                .toArray();

        double bestChkInt = -1;
        double bestOptValue = -1;

        for (int i = 0; i < chkIntArr.length; i++) {

            double predLat = predLatArr[i];
            double predRec = predRecArr[i];

            double latRatio = (predLat * weight) / context.avgLatConst;
            double recRatio = predRec / context.recTimeConst;
            double optValue = latRatio + recRatio + Math.abs(latRatio - recRatio);
            if (0 < predLat && latRatio < 1 &&
                0 < predRec && recRatio < 1 && (optValue < bestOptValue || bestOptValue == -1)) {

                if (type.equalsIgnoreCase("LATENCY") && context.targetJob.getConfig() < chkIntArr[i]) {

                    LOG.info("chkInt: " + chkIntArr[i] + ", predLat: " + predLat + "(" + predLat * weight + "), predRec:" + predRec + ", optValue: " + optValue + " (" + latRatio + ", " + recRatio + ", " + Math.abs(latRatio - recRatio) + ")");
                    bestChkInt = chkIntArr[i];
                    bestOptValue = optValue;
                }
                else if (type.equalsIgnoreCase("RECTIME") && chkIntArr[i] < context.targetJob.getConfig()) {

                    LOG.info("chkInt: " + chkIntArr[i] + ", predLat: " + predLat + "(" + predLat * weight + "), predRec:" + predRec + ", optValue: " + optValue + " (" + latRatio + ", " + recRatio + ", " + Math.abs(latRatio - recRatio) + ")");
                    bestChkInt = chkIntArr[i];
                    bestOptValue = optValue;
                }
            }
        }
        LOG.info("Chosen best checkpoint interval: " + bestChkInt);
        return bestChkInt;
    }
}
