package de.tu_berlin.dos.arm.khaos.workload_manager;

import com.google.gson.JsonParser;
import de.tu_berlin.dos.arm.khaos.common.utils.DateUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;

public class FileToQueueManager implements Runnable {

    /******************************************************************************
     * STATIC VARIABLES
     ******************************************************************************/

    private static final Logger LOG = Logger.getLogger(QueueToKafkaManager.class);

    /******************************************************************************
     * INSTANCE STATE
     ******************************************************************************/

    private final File inputFile;
    private final BlockingQueue<List<String>> queue;

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    public FileToQueueManager(File inputFile, BlockingQueue<List<String>> queue) {

        this.inputFile = inputFile;
        this.queue = queue;
    }

    /******************************************************************************
     * INSTANCE BEHAVIOURS
     ******************************************************************************/

    @Override
    public void run() {

        try (Scanner sc = new Scanner(new FileInputStream(inputFile), StandardCharsets.UTF_8)) {
            // initialize loop values
            List<String> events = null;
            Timestamp head = null;
            // loop reading through file line by line
            while (sc.hasNextLine()) {
                // extract timestamp from line
                String line = sc.nextLine();
                String tsString = JsonParser.parseString(line).getAsJsonObject().get("ts").getAsString();
                Timestamp current = new Timestamp(DateUtil.provideDateFormat().parse(tsString).getTime());
                // test if it is the first iteration
                if (head == null) {

                    head = current;
                    events = new ArrayList<>();
                    events.add(line);
                }
                // test if timestamps match
                else if (head.compareTo(current) == 0) {

                    events.add(line);
                }
                // test if timestamps do not match
                else if (head.compareTo(current) != 0) {

                    head = current;
                    queue.put(events);
                    events = new ArrayList<>();
                    events.add(line);
                }
            }
        }
        catch (ParseException | IOException | InterruptedException ex) {

            LOG.error(ex);
        }
    }
}
