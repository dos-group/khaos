package de.tu_berlin.dos.arm.khaos.workload_manager.modeling;

import de.tu_berlin.dos.arm.khaos.common.data.TimeSeries;
import de.tu_berlin.dos.arm.khaos.workload_manager.Context;
import iftm.anomalydetection.AutoMLAnomalyDetection;
import iftm.anomalydetection.automl.identityfunctions.AbstractArimaBreeding;
import iftm.anomalydetection.automl.identityfunctions.ArimaMultiBreeding;
import iftm.anomalydetection.automl.identityfunctions.CompleteHistoryThresholdBreedingPart;
import iftm.anomalydetection.automl.identityfunctions.ThresholdBreedingPart;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;

public class AnomalyDetector {

    private static final Logger LOG = Logger.getLogger(AnomalyDetector.class);

    private final AutoMLAnomalyDetection detector;

    public AnomalyDetector() {

        ThresholdBreedingPart breeding1 = new CompleteHistoryThresholdBreedingPart(5.0);
        AbstractArimaBreeding breeding2 = new ArimaMultiBreeding(50, 4, 200, breeding1);
        this.detector = new AutoMLAnomalyDetection(breeding2, 1000, false);
    }

    public void fit(List<TimeSeries> trainingSet, int trainingSize, long injectionTimestamp) {

        // extract values based on time window and injection time point
        for (int i = 0; i < trainingSize; i++) {

            // for current timestamp, extract training data points
            boolean proceed = true;
            long currTimestamp = injectionTimestamp - trainingSize + i;
            TimeSeries currTimeSeries;
            double[] currDataPoints = new double[trainingSet.size()];
            for (int j = 0; j < trainingSet.size(); j++) {

                currTimeSeries = trainingSet.get(j);
                double value = currTimeSeries.getObservation(currTimestamp).value;
                if (Double.isNaN(value)) {
                    proceed = false;
                    break;
                }
                else currDataPoints[j] = value;
            }
            // if no NaN values were detected in training points, then train the detector
            if (proceed) {

                LOG.info("Train datapoint " + (i + 1) + " of " + trainingSize + " ... " + Arrays.toString(currDataPoints));
                this.detector.train(currDataPoints);
            }
        }
    }
}
