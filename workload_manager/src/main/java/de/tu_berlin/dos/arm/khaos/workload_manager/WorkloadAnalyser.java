package de.tu_berlin.dos.arm.khaos.workload_manager;

import com.google.gson.JsonParser;
import de.tu_berlin.dos.arm.khaos.common.utils.DateUtil;
import org.apache.commons.lang3.StringUtils;
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

    public static WorkloadAnalyser createFromWorkloadFile(String inputFilePath) {

        File inputFile = new File(inputFilePath);

        // create arrays for workload
        List<Tuple2<Integer, Integer>> workload = new ArrayList<>();

        boolean isFirst = true;
        try (Scanner sc = new Scanner(new FileInputStream(inputFile), StandardCharsets.UTF_8)) {

            while (sc.hasNextLine()) {

                String line = sc.nextLine();
                if (isFirst) {
                    isFirst = false;
                    continue;
                }
                String[] split = StringUtils.split(line,"|");
                workload.add(new Tuple2<>(Integer.parseInt(split[0]), Integer.parseInt(split[1])));
            }

            return new WorkloadAnalyser(workload);
        }
        catch (FileNotFoundException e) {

            throw new IllegalStateException(e.getMessage());
        }
    }

    public static WorkloadAnalyser createFromEventsFile(String inputFilePath) {

        File inputFile = new File(inputFilePath);

        // create arrays for workload
        List<Tuple2<Integer, Integer>> workload = new ArrayList<>();

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

                    workload.add(new Tuple2<>(secondCount, eventsCount));
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

    private final List<Tuple2<Integer, Integer>> workload;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    private WorkloadAnalyser(List<Tuple2<Integer, Integer>> workload) {

        this.workload = workload;
    }

    /******************************************************************************
     * INSTANCE BEHAVIOURS
     ******************************************************************************/

    public void printWorkload(File output) throws IOException {

        FileWriter fw = new FileWriter(output);

        fw.write("second|throughput");
        for (int i = 1; i <= this.workload.size(); i++) {

            fw.write(this.workload.get(i)._1() + "|" + this.workload.get(i)._2() + "\n");
        }

        fw.close();
    }

    public List<Tuple2<Integer, Integer>> getFailureScenario() {

        return Arrays.asList(
            new Tuple2<>(2000,486),
            new Tuple2<>(78525,6440),
            new Tuple2<>(43067,12676),
            new Tuple2<>(56336,18647),
            new Tuple2<>(24104,24148),
            new Tuple2<>(74633,28241),
            new Tuple2<>(29301,32276),
            new Tuple2<>(61343,39559),
            new Tuple2<>(63798,45640),
            new Tuple2<>(64923,52404));
    }

    public List<Tuple2<Integer, Integer>> getFailureScenario(
            int minFailureInterval, int averagingWindowSize, int numFailures) {

        LOG.info(this.workload.size());

        // find list of the max and minimum points
        List<Tuple2<Integer, Integer>> min = new ArrayList<>();
        List<Tuple2<Integer, Integer>> max = new ArrayList<>();
        System.out.println(workload.size());
        for (int i = minFailureInterval; i < this.workload.size() - minFailureInterval; i++) {
            int sum = 0;
            for (int j = i - averagingWindowSize + 1; j <= i; j++) {
                sum += this.workload.get(j)._2();
            }
            int average = sum / averagingWindowSize;
            if (min.isEmpty()) min.add(this.workload.get(i));
            else if (min.get(0)._2() == average) min.add(this.workload.get(i));
            else if (average < min.get(0)._2()) {
                min.clear();
                min.add(this.workload.get(i));
            }
            if (max.isEmpty()) max.add(this.workload.get(i));
            else if (max.get(0)._2() == average) max.add(this.workload.get(i));
            else if (max.get(0)._2() < average) {
                max.clear();
                max.add(this.workload.get(i));
            }
        }
        // test if max and min were found
        if (min.size() == 0 || max.size() == 0) throw new IllegalStateException("Unable to find Max and/or Min intersects");
        // find values for average throughput where failures should be injected
        int maxVal = max.get(0)._2();
        int minVal = min.get(0)._2();
        int step = (int) (((maxVal - minVal) * 1.0 / (numFailures - 1)) + 0.5);
        // create map of for all points of intersection
        Map<Integer, List<Tuple2<Integer, Integer>>> intersects = new TreeMap<>();
        intersects.put(minVal, min);
        Stream.iterate(minVal, i -> i + step).limit(numFailures - 1).forEach(i -> intersects.putIfAbsent(i, new ArrayList<>()));
        intersects.put(maxVal, max);
        // find all points of intersection in workload
        for (int i = minFailureInterval; i < this.workload.size() - minFailureInterval; i++) {

            int sum = 0;
            for (int j = i - averagingWindowSize + 1; j <= i; j++) {

                sum += this.workload.get(j)._2();
            }
            int average = sum / averagingWindowSize;
            // find range of values which is 1% of average
            int onePercent = (int) ((this.workload.size() / 100) + 0.5);
            List<Integer> targetRange =
                IntStream
                    .rangeClosed(average - onePercent , average + onePercent)
                    .boxed()
                    .collect(Collectors.toList());
            // find all intersects that are in 1% range of target
            for (int valueInRange : targetRange) {
                if (intersects.containsKey(valueInRange))
                    intersects.get(valueInRange).add(this.workload.get(i));
            }
        }
        LOG.info(intersects.size());
        // test if enough points were found at each intersect
        intersects.forEach((k,v) -> {
            if (v.size() == 0) throw new IllegalStateException("Unable to find intersect for " + k);
        });
        // randomly select combination of intersects where at least one full set exists
        List<Tuple2<Integer, Integer>> scenario = new ArrayList<>();
        Random rand = new Random();
        StopWatch timer = new StopWatch();
        timer.start();
        while (true) {
            intersects.forEach((k,v) -> scenario.add(v.get(rand.nextInt(v.size()))));
            boolean valid = true;
            for (int i = 0; i < scenario.size(); i++) {
                for (int j = i+1; j < scenario.size(); j++) {
                    if (Math.abs(scenario.get(i)._2() - scenario.get(j)._2()) < minFailureInterval) {
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
