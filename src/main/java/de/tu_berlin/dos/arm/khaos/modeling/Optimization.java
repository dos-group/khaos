package de.tu_berlin.dos.arm.khaos.modeling;

import de.tu_berlin.dos.arm.khaos.core.Context;

import java.util.Arrays;

public class Optimization {

    private Optimization() {}

    public static double getOptimalCheckpointInterval(Context context, double currentThr, double[] checkpointIntervals){

        // TODO FIX THIS
        double[] predLatencies = Arrays.stream(checkpointIntervals)
                //.map(i -> context.performance.predict(Arrays.asList(i, currentThr)))
                .toArray();

        double[] predRecoveryTimes = Arrays.stream(checkpointIntervals)
                //.map(i -> context.availability.predict(Arrays.asList(i, currentThr)))
                .toArray();

        double bestCheckpointInterval = -1;
        double bestRatioSum = -1;

        for (int i = 0; i < checkpointIntervals.length; i++) {
            double predLatency = predLatencies[i];
            double predRecoveryTime = predRecoveryTimes[i];

            double ratioSum = (predLatency / context.avgLatConst) + (predRecoveryTime / context.recTimeConst);
            if(predLatency < context.avgLatConst &&
                    predRecoveryTime < context.recTimeConst &&
                    predLatency > 0 &&
                    predRecoveryTime > 0 && (bestRatioSum == -1 || ratioSum < bestRatioSum)){
                bestCheckpointInterval = checkpointIntervals[i];
                bestRatioSum = ratioSum;
            }
        }

        return bestCheckpointInterval;
    }
}
