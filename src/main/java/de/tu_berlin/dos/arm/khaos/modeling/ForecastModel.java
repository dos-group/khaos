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

    public ForecastModel(int p, int d, int q, boolean constant, String strategy){
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

    public double[] predict(int steps){
        if (this.model == null) throw new IllegalStateException("Fit the model first");

        return this.model.fcst(steps);
    }

    public static void main(String[] args){
        ForecastModel model = new ForecastModel(2,2,2, false, "CSSML");

        double[] history = {1.0, 2.0 ,1.0 ,3.0 };
        double[] forecast = model.fit(history).predict(10);
        System.out.println(Arrays.toString(forecast));
    }
}
