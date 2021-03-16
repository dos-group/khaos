package de.tu_berlin.dos.arm.khaos.anomaly_detector;

import de.tu_berlin.dos.arm.khaos.common.data.TimeSeries;
import de.tu_berlin.dos.arm.khaos.common.utils.FileParser;
import de.tu_berlin.dos.arm.khaos.common.utils.FileReader;
import iftm.anomalydetection.AutoMLAnomalyDetection;
import iftm.anomalydetection.DistancePredictionResult;
import iftm.anomalydetection.automl.identityfunctions.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.*;

public class Run {

    public static final Logger LOG = Logger.getLogger(Run.class);


    public static void main(String[] args) throws Exception {

        int minFailureWindow = 1000;
        int tolerance = 10;

        List<Long> failurePoints =
            Arrays.asList(
                1615553479L, 1615575588L, 1615580781L, 1615594548L, 1615607816L,
                1615612826L, 1615615276L, 1615616403L, 1615626120L, 1615630005L);

        // create time series from file
        File thrFile = FileReader.GET.read("iot_8_thr_vehicles-cId5yU0Jb1.csv", File.class);
        File lagFile = FileReader.GET.read("iot_8_lag_vehicles-cId5yU0Jb1.csv", File.class);
        TimeSeries thrTS = new TimeSeries(FileParser.GET.csv(thrFile, "\\|", true), 86400);
        TimeSeries lagTS = new TimeSeries(FileParser.GET.csv(lagFile, "\\|", true), 86400);

        // create and optimize anomaly detector
        ThresholdBreedingPart breeding1 = new CompleteHistoryThresholdBreedingPart(5.0);
        AbstractArimaBreeding breeding2 = new ArimaMultiBreeding(50, 4, 200, breeding1);
        AutoMLAnomalyDetection detector = new AutoMLAnomalyDetection(breeding2, 1000, false);

        // extract values based on time window and injection time point
        for (int i = 0; i < minFailureWindow; i++) {

            // for current timestamp, extract values
            long currTS = 1615616403L - minFailureWindow + i;
            double thrVal = thrTS.getObservation(currTS).value;
            double lagVal = lagTS.getObservation(currTS).value;

            // train only if values are found
            if (!Double.isNaN(thrVal) || !Double.isNaN(lagVal)) {

                LOG.info("Train datapoint " + (i + 1) + " of " + minFailureWindow + " ... " + thrVal + " " + lagVal);
                detector.train(new double[]{thrVal, lagVal});
            }
        }
        boolean anomaly = false;
        int duration = 0;
        int history = 0;

        int count = 1;
        while (true) {

            long currTS = 1615616403L + count;
            int thrIdx = thrTS.getIndex(currTS);
            int lagIdx = lagTS.getIndex(currTS);

            // test if current timestamp exists in metrics
            if (thrIdx < 0 || lagIdx < 0) {

                anomaly = true;
                history = 0;
                duration += 1;
                LOG.info(currTS + " value missing, therefore anomaly");
            }
            // if it does exist, then retrieve those values and process
            else {

                double thrVal = thrTS.observations.get(thrIdx).value;
                double lagVal = lagTS.observations.get(lagIdx).value;

                DistancePredictionResult result = detector.predict(new double[]{thrVal, lagVal});
                LOG.info(thrVal + " " + lagVal + " " + result.isAnomaly());
                if (!result.isAnomaly() && !anomaly) detector.train(new double[]{thrVal, lagVal});
                else if (result.isAnomaly()) {

                    anomaly = true;
                    history = 0;
                    duration += 1;
                }
                else if (!result.isAnomaly()) {

                    // if tolerance is exceeded, then break (its very likely that the anomaly actually terminated)
                    if (tolerance < history) {

                        duration -= history;
                        LOG.info(1615553479L + " " + duration);
                        break;
                    }
                    // some tolerance (numbers of datapoints), i.e. if its actually an anomaly but the detector failed
                    else {
                        anomaly = true;
                        history += 1;
                        duration += 1;
                    }
                }
            }
            count++;
        }
    }
}
