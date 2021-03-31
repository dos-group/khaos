package de.tu_berlin.dos.arm.khaos.modeling;

import de.tu_berlin.dos.arm.khaos.io.TimeSeries;
import iftm.anomalydetection.AutoMLAnomalyDetection;
import iftm.anomalydetection.DistancePredictionResult;
import iftm.anomalydetection.automl.identityfunctions.AbstractArimaBreeding;
import iftm.anomalydetection.automl.identityfunctions.ArimaMultiBreeding;
import iftm.anomalydetection.automl.identityfunctions.CompleteHistoryThresholdBreedingPart;
import iftm.anomalydetection.automl.identityfunctions.ThresholdBreedingPart;
import org.apache.log4j.Logger;

import java.util.List;

public class AnomalyDetector {

    private static final Logger LOG = Logger.getLogger(AnomalyDetector.class);
    private static final int TOLERANCE = 10;

    private final AutoMLAnomalyDetection detector;
    private final List<TimeSeries> dataset;
    private boolean trained = false;

    public AnomalyDetector(List<TimeSeries> dataset) {

        ThresholdBreedingPart breeding1 = new CompleteHistoryThresholdBreedingPart(5.0);
        AbstractArimaBreeding breeding2 = new ArimaMultiBreeding(50, 4, 200, breeding1);
        this.detector = new AutoMLAnomalyDetection(breeding2, 1000, false);
        this.dataset = dataset;
    }

    public AnomalyDetector fit(long injectionTimestamp, int trainingSize) {

        LOG.info("Training Started");
        // extract values based on time window and injection time point
        for (int i = 0; i < trainingSize; i++) {

            // for current timestamp, extract training data points
            boolean proceed = true;
            long currTimestamp = injectionTimestamp - trainingSize + i;
            double[] currDataPoints = new double[dataset.size()];
            for (int j = 0; j < dataset.size(); j++) {

                TimeSeries currTimeSeries = dataset.get(j);
                double value = currTimeSeries.getObservation(currTimestamp).value;
                if (Double.isNaN(value)) {
                    proceed = false;
                    break;
                }
                else currDataPoints[j] = value;
            }
            // if no NaN values were detected in training points, then train the detector
            if (proceed) {

                //LOG.info("Train datapoint " + (i + 1) + " of " + trainingSize + " ... " + Arrays.toString(currDataPoints));
                this.detector.train(currDataPoints);
            }
        }
        this.trained = true;
        LOG.info("Training finished");
        return this;
    }

    // TODO add timeout
    public double measure(long injectionTimestamp, int timeout) {

        if (!this.trained) throw new IllegalStateException("Train the Anomaly Detector first");

        boolean anomaly = false;
        double duration = 0;
        double history = 0;

        // extract data points for current timestamp
        int count = 1;
        while (true) {

            boolean proceed = true;
            long currTimestamp = injectionTimestamp + count;
            double[] currDataPoints = new double[dataset.size()];
            for (int i = 0; i < dataset.size(); i++) {

                TimeSeries currTimeSeries = dataset.get(i);
                double value = currTimeSeries.getObservation(currTimestamp).value;
                if (Double.isNaN(value)) {
                    proceed = false;
                    anomaly = true;
                    history = 0;
                    duration += 1;
                    break;
                }
                else currDataPoints[i] = value;
            }
            // proceed only if no NaN values were found
            if (proceed) {

                DistancePredictionResult result = detector.predict(currDataPoints);
                if (!result.isAnomaly() && !anomaly) detector.train(currDataPoints);
                else if (result.isAnomaly()) {

                    anomaly = true;
                    history = 0;
                    duration += 1;
                }
                else if (!result.isAnomaly()) {

                    // if tolerance is exceeded, then break (its very likely that the anomaly actually terminated)
                    if (TOLERANCE < history) {

                        return duration - history;
                    }
                    // some tolerance (numbers of datapoints), i.e. if its actually an anomaly but the detector failed
                    else {
                        history += 1;
                        duration += 1;
                    }
                }
            }
            count++;
        }
    }
}
