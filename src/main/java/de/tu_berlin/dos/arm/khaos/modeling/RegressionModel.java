package de.tu_berlin.dos.arm.khaos.modeling;

import org.apache.log4j.Logger;
import scala.Tuple2;
import scala.Tuple3;
import smile.data.DataFrame;
import smile.data.formula.Formula;
import smile.regression.LinearModel;
import smile.regression.OLS;

import java.util.*;

public class RegressionModel {

    private static final Logger LOG = Logger.getLogger(RegressionModel.class);

    private LinearModel model;

    public RegressionModel() { }

    public RegressionModel fit(Tuple3<String, String, String> header, double [][] dataArr, String lhs) {

        // create dataframe
        DataFrame df = DataFrame.of(dataArr, header._1(), header._2(), header._3());
        // fit the model
        this.model = OLS.fit(Formula.lhs(lhs), df);
        LOG.info(this.model);
        LOG.info(Arrays.toString(this.model.coefficients()));
        return this;
    }

    public RegressionModel fit(Tuple3<String, String, String> header, List<Tuple3<Double, Double, Double>> data, String lhs) {

        // convert to multi-dimensional array
        double [][] dataArr = new double[data.size()][];
        for (int i = 0; i < data.size(); i++) {

            dataArr[i] = new double[]{data.get(i)._1(), data.get(i)._2(), data.get(i)._3()};
        }
        return this.fit(header, dataArr, lhs);
    }

    public double[] predict(String header1, double val1, String header2, double val2) {

        double[][] values = new double[1][];
        values[0] = new double[]{val1, val2};
        DataFrame df = DataFrame.of(values, header1, header2);
        return this.model.predict(df);
    }

    public LinearModel getModel() {

        return model;
    }
}
