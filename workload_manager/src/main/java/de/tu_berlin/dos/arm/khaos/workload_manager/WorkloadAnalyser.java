package de.tu_berlin.dos.arm.khaos.workload_manager;

import com.google.gson.JsonParser;
import de.tu_berlin.dos.arm.khaos.common.utils.DateUtil;
import de.tu_berlin.dos.arm.khaos.common.utils.FileReader;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.analysis.interpolation.SplineInterpolator;
import org.apache.commons.math3.analysis.polynomials.PolynomialSplineFunction;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkloadAnalyser {

    /******************************************************************************
     * CLASS VARIABLES
     ******************************************************************************/

    private static final Logger LOG = Logger.getLogger(WorkloadAnalyser.class);

    /******************************************************************************
     * CLASS BEHAVIOURS
     ******************************************************************************/

    public static WorkloadAnalyser create(String inputFilePath, int minTimeBetweenFailures, int numOfFailures) {

        File inputFile = new File(inputFilePath);

        // create arrays for workload
        List<Double> x = new ArrayList<>();
        List<Double> y = new ArrayList<>();

        try (Scanner sc = new Scanner(new FileInputStream(inputFile), StandardCharsets.UTF_8)) {
            // initialize loop values
            double xCounter = 0;
            double yCounter = 0;
            Timestamp head = null;
            // loop reading through file line by line
            while (sc.hasNextLine()) {
                // extract timestamp from line
                String line = sc.nextLine();
                String tsString = JsonParser.parseString(line).getAsJsonObject().get("ts").getAsString();
                Timestamp current = new Timestamp(DateUtil.provideDateFormat().parse(tsString).getTime());
                // test if it is the first iteration
                if (head == null) {

                    xCounter = 1;
                    yCounter = 1;
                    head = current;
                }
                // test if timestamps match
                else if (head.compareTo(current) == 0) {

                    yCounter++;
                }
                // test if timestamps do not match
                else if (head.compareTo(current) != 0) {

                    head = current;
                    x.add(xCounter);
                    y.add(yCounter);
                    xCounter++;
                    yCounter = 1;
                }
            }
            return new WorkloadAnalyser(minTimeBetweenFailures, numOfFailures, x, y);
        }
        catch (ParseException | FileNotFoundException ex) {

            throw new IllegalStateException(ex.getMessage());
        }
    }

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    private final int minTimeBetweenFailures;
    private final int numOfFailures;
    private final List<Double> x;
    private final List<Double> y;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    private WorkloadAnalyser(int minTimeBetweenFailures, int numOfFailures, List<Double> x, List<Double> y) {

        this.minTimeBetweenFailures = minTimeBetweenFailures;
        this.numOfFailures = numOfFailures;
        this.x = x;
        this.y = y;
    }

    /******************************************************************************
     * INSTANCE BEHAVIOURS
     ******************************************************************************/

    public void getFailureScenario() {


    }

    public PolynomialSplineFunction interpolate() {

        double[] xArr = ArrayUtils.toPrimitive(this.x.toArray(new Double[0]));
        double[] yArr = ArrayUtils.toPrimitive(this.y.toArray(new Double[0]));

        return new SplineInterpolator().interpolate(xArr, yArr);
    }

    public int getMaxY() {

        return Collections.max(this.y).intValue();
    }

    public List<Double> getX() {

        return x;
    }

    public List<Double> getY() {

        return y;
    }
}
