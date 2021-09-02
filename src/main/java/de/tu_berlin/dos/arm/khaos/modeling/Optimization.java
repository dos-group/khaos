package de.tu_berlin.dos.arm.khaos.modeling;

import de.tu_berlin.dos.arm.khaos.core.Context;
import org.apache.log4j.Logger;

import java.util.Arrays;

public class Optimization {

    private static final Logger LOG = Logger.getLogger(Optimization.class);

    private Optimization() {}

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
            if (0 < predLat && latRatio < 1 && 0 < predRec && recRatio < 1 && (optValue < bestOptValue || bestOptValue == -1)) {

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
