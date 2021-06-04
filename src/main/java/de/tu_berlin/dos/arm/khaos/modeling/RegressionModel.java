package de.tu_berlin.dos.arm.khaos.modeling;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.commons.math3.util.FastMath;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class RegressionModel extends OLSMultipleLinearRegression {

    private final OLSMultipleLinearRegression regression;
    private double[] parameters;

    public RegressionModel() {

        this.regression = new OLSMultipleLinearRegression();
    }

    public RegressionModel fit(List<Double> y, List<List<Double>> x) {

        //regression.setNoIntercept(true);
        double[] y_arr = ArrayUtils.toPrimitive(y.toArray(new Double[0]));
        double[][] x_arr = new double[x.size()][];
        for (int i = 0; i < x.size(); i++) {

            x_arr[i] = ArrayUtils.toPrimitive(x.get(i).toArray(new Double[0]));
        }
        this.regression.newSampleData(y_arr, x_arr);
        this.parameters = this.regression.estimateRegressionParameters();

        return this;
    }

    public double calculateRSquared() {

        return this.regression.calculateRSquared();
    }

    public List<Tuple3<Double, Double, Double>> calculatePValues() {

        final double[] beta = regression.estimateRegressionParameters();
        int residualdf = this.regression.estimateResiduals().length - beta.length;
        List<Tuple3<Double, Double, Double>> pValues = new ArrayList<>();
        for (int i = 0; i < beta.length; i++) {

            double tStat = beta[i] / regression.estimateRegressionParametersStandardErrors()[i];
            double pValue = new TDistribution(residualdf).cumulativeProbability(-FastMath.abs(tStat)) * 2;
            pValues.add(new Tuple3<>(beta[i], tStat, pValue));
        }
        return pValues;
    }

    public double predict(List<Double> args) {

        if (this.parameters == null) throw new IllegalStateException("Fit the model first");

        double[] args_arr = ArrayUtils.toPrimitive(args.toArray(new Double[0]));
        double result = 0;
        for (int i = 0; i < parameters.length; i++) {

            if (i == 0) result += parameters[i]; // y-intercept
            else result += parameters[i] * args_arr[i-1];
        }
        return result;
    }
}
