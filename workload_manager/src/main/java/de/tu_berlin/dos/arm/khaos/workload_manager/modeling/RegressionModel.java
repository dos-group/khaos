package de.tu_berlin.dos.arm.khaos.workload_manager.modeling;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

import java.util.List;

public class RegressionModel {

    private final OLSMultipleLinearRegression regression;
    private double[] parameters;

    public RegressionModel() {

        this.regression = new OLSMultipleLinearRegression();
    }

    public RegressionModel fit(List<Double> y, List<List<Double>> x) {

        double[] y_arr = ArrayUtils.toPrimitive(y.toArray(new Double[0]));
        double[][] x_arr = new double[x.size()][];
        for (int i = 0; i < x.size(); i++) {

            x_arr[i] = ArrayUtils.toPrimitive(x.get(i).toArray(new Double[0]));
        }
        this.regression.newSampleData(y_arr, x_arr);
        this.parameters = this.regression.estimateRegressionParameters();
        return this;
    }
    public double predict(List<Double> args) {

        if (this.parameters == null) throw new IllegalStateException("Fit the model first");

        double[] args_arr = ArrayUtils.toPrimitive(args.toArray(new Double[0]));
        double result = 0;
        for(int i = 0; i < parameters.length; i++) {
            result += parameters[i] * args_arr[i];
        }
        return result;
    }
}
