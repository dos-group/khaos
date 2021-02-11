package de.tu_berlin.dos.arm.khaos.anomaly_detector;

import iftm.anomalydetection.AutoMLAnomalyDetection;
import iftm.anomalydetection.DistancePredictionResult;
import iftm.anomalydetection.automl.identityfunctions.*;
import org.junit.Assert;

import java.util.Random;

public class Run {

    public static void main(String[] args) {

        Random random = new Random();

        ThresholdBreedingPart breeding1 = new CompleteHistoryThresholdBreedingPart(5.0);
        ThresholdBreedingPart breeding2 = new ExponentialMovingThresholdBreedingPart(5.0, 0.01);
        AbstractArimaBreeding breeding3 = new ArimaMultiBreeding(50, 4, 200, breeding1);
        AutoMLAnomalyDetection detector = new AutoMLAnomalyDetection(breeding3, 1000, false);

        double[] datapoint1 = new double[]{0.0,0.3,2.0};
        detector.train(datapoint1);

        double[] datapoint2 = new double[]{0.0,100.0,2.0};
        DistancePredictionResult result = detector.predict(datapoint2);
        System.out.println("Is Anomaly: "+result.isAnomaly());

        Exception ex = null;
        try {
            for (int i = 0; i < 1000;i++) {
                detector.predict(new double[]{100 * random.nextDouble(), 100 * random.nextDouble(), 100 * random.nextDouble()});
                detector.train(new double[]{100 * random.nextDouble(), 100 * random.nextDouble(), 100 * random.nextDouble()});
            }
        }
        catch (Exception e) {
            ex = e;
        }
        Assert.assertNull(ex);
    }
}
