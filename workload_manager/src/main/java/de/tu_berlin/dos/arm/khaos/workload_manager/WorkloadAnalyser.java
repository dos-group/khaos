package de.tu_berlin.dos.arm.khaos.workload_manager;

import com.google.gson.JsonParser;
import de.tu_berlin.dos.arm.khaos.common.utils.DateUtil;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.Logger;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class WorkloadAnalyser {

    /******************************************************************************
     * CLASS VARIABLES
     ******************************************************************************/

    private static final Logger LOG = Logger.getLogger(WorkloadAnalyser.class);

    /******************************************************************************
     * CLASS BEHAVIOURS
     ******************************************************************************/

    public static WorkloadAnalyser create(String inputFilePath) {

        File inputFile = new File(inputFilePath);

        // create arrays for workload
        List<Tuple3<Integer, Timestamp, Integer>> workload = new ArrayList<>();

        try (Scanner sc = new Scanner(new FileInputStream(inputFile), StandardCharsets.UTF_8)) {
            // initialize loop values
            Integer secondCount = null;
            Integer eventsCount = null;
            Timestamp head = null;
            // loop reading through file line by line
            while (sc.hasNextLine()) {
                // extract timestamp from line
                String line = sc.nextLine();
                String tsString = JsonParser.parseString(line).getAsJsonObject().get("ts").getAsString();
                Timestamp current = new Timestamp(DateUtil.provideDateFormat().parse(tsString).getTime());
                // test if it is the first iteration
                if (head == null) {

                    secondCount = 1;
                    eventsCount = 1;
                    head = current;
                }
                // test if timestamps match
                else if (head.compareTo(current) == 0) {

                    eventsCount++;
                }
                // test if timestamps do not match
                else if (head.compareTo(current) != 0) {

                    workload.add(new Tuple3<>(secondCount, head, eventsCount));
                    secondCount++;
                    head = current;
                    eventsCount = 1;
                }
            }
            return new WorkloadAnalyser(workload);
        }
        catch (ParseException | FileNotFoundException ex) {

            throw new IllegalStateException(ex.getMessage());
        }
    }

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    private final List<Tuple3<Integer, Timestamp, Integer>> workload;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    private WorkloadAnalyser(List<Tuple3<Integer, Timestamp, Integer>> workload) {

        this.workload = workload;
    }

    /******************************************************************************
     * INSTANCE BEHAVIOURS
     ******************************************************************************/

    public void printWorkload(File output) throws IOException {

        FileWriter fw = new FileWriter(output);

        fw.write("second|throughput");
        for (int i = 1; i <= this.workload.size(); i++) {

            fw.write(this.workload.get(i)._1() + "|" + this.workload.get(i)._3());
        }

        fw.close();
    }

    public List<Tuple3<Integer, Timestamp, Integer>> getFailureScenario(
            int minFailureInterval, int averagingWindowSize, int numFailures) {

        System.out.println(this.workload.size());

        // find list of the max and minimum points
        List<Tuple3<Integer, Timestamp, Integer>> min = new ArrayList<>();
        List<Tuple3<Integer, Timestamp, Integer>> max = new ArrayList<>();
        for (int i = minFailureInterval; i < this.workload.size() - minFailureInterval; i++) {
            int sum = 0;
            for (int j = i - averagingWindowSize + 1; j <= i; j++) {
                sum += this.workload.get(j)._3();
            }
            int average = sum / averagingWindowSize;
            if (min.isEmpty()) min.add(this.workload.get(i));
            else if (min.get(0)._3() == average) min.add(this.workload.get(i));
            else if (average < min.get(0)._3()) {
                min.clear();
                min.add(this.workload.get(i));
            }
            if (max.isEmpty()) max.add(this.workload.get(i));
            else if (max.get(0)._3() == average) max.add(this.workload.get(i));
            else if (max.get(0)._3() < average) {
                max.clear();
                max.add(this.workload.get(i));
            }
        }
        // test if max and min were found
        if (min.size() == 0 || max.size() == 0) throw new IllegalStateException("Unable to find Max and/or Min intersects");
        // find values for average throughput where failures should be injected
        int maxVal = max.get(0)._3();
        int minVal = min.get(0)._3();
        int step = (int) (((maxVal - minVal) * 1.0 / (numFailures - 1)) + 0.5);
        // create map of for all points of intersection
        Map<Integer, List<Tuple3<Integer, Timestamp, Integer>>> intersects = new TreeMap<>();
        intersects.put(minVal, min);
        Stream.iterate(minVal, i -> i + step).limit(numFailures - 1).forEach(i -> intersects.putIfAbsent(i, new ArrayList<>()));
        intersects.put(maxVal, max);
        // find all points of intersection in workload
        for (int i = minFailureInterval; i < this.workload.size() - minFailureInterval; i++) {

            int sum = 0;
            for (int j = i - averagingWindowSize + 1; j <= i; j++) {

                sum += this.workload.get(j)._3();
            }
            int average = sum / averagingWindowSize;
            //LOG.info(sum + " " + average);
            if (intersects.containsKey(average)) {
                intersects.get(average).add(this.workload.get(i));
            }
        }
        // test if enough points were found at each intersect
        intersects.forEach((k,v) -> {
            if (v.size() == 0) throw new IllegalStateException("Unable to find intersect for " + k);
        });
        // randomly select combination of intersects where at least one full set exists
        List<Tuple3<Integer, Timestamp, Integer>> scenario = new ArrayList<>();
        Random rand = new Random();
        StopWatch timer = new StopWatch();
        timer.start();
        while (true) {
            intersects.forEach((k,v) -> scenario.add(v.get(rand.nextInt(v.size()))));
            boolean valid = true;
            for (int i = 0; i < scenario.size(); i++) {
                for (int j = i+1; j < scenario.size(); j++) {
                    if (Math.abs(scenario.get(i)._3() - scenario.get(j)._3()) < minFailureInterval) {
                        valid = false;
                        break;
                    }
                }
            }
            if (valid) return scenario;
            if (timer.getTime(TimeUnit.SECONDS) > 120)
                throw new IllegalStateException("Unable to find combination of intersects");
        }
    }
}
