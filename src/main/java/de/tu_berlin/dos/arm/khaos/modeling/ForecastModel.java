package de.tu_berlin.dos.arm.khaos.modeling;

import timeseries.TimeSeries;
import timeseries.models.arima.Arima;
import timeseries.models.arima.Arima.ModelOrder;
import timeseries.models.arima.Arima.FittingStrategy;

import java.util.Arrays;

public class ForecastModel {

    private final ModelOrder order;
    private final FittingStrategy strategy;
    private Arima model;

    public ForecastModel(int p, int d, int q, boolean constant, String strategy) {

        // Non-seasonal ARIMA model configuration
        this.order = Arima.order(p, d, q, constant);
        // The strategy to be used for fitting an ARIMA model
        this.strategy = FittingStrategy.valueOf(strategy);
    }

    public ForecastModel fit(double[] values) {

        TimeSeries observations = new TimeSeries(values);
        this.model = Arima.model(observations, this.order, this.strategy);
        return this;
    }

    public double[] predict(int steps) {

        if (this.model == null) throw new IllegalStateException("Fit the model first");
        return this.model.fcst(steps);
    }

    public static double computeDifferences(double lastThr, double[] predThrs) {

        double mySum = 0;
        for (int i = 1; i < predThrs.length; i++) {

            if (i == 1) {
                mySum += predThrs[i] - lastThr;
            }
            else {
                mySum += predThrs[i] - predThrs[i-1];
            }
        }
        return mySum;
    }
}
