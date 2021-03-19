package de.tu_berlin.dos.arm.khaos.common.utils;

import de.tu_berlin.dos.arm.khaos.common.data.Observation;
import de.tu_berlin.dos.arm.khaos.common.data.TimeSeries;
import org.apache.log4j.Logger;

import java.io.*;
import java.io.FileReader;
import java.util.*;

public enum FileParser { GET;

    public static final Logger LOG = Logger.getLogger(FileParser.class);

    public TimeSeries fromCSV(File file, String sep, boolean header) throws Exception {

        List<String> lines = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            boolean headerRead = false;
            while ((line = br.readLine()) != null) {
                if (header && !headerRead) {
                    headerRead = true;
                    continue;
                }
                lines.add(line);
            }
        }
        // create time series using first and last element
        TimeSeries ts =
            TimeSeries.create(
                Long.parseLong(lines.get(0).split(sep)[0]),
                Long.parseLong(lines.get(lines.size() - 1).split(sep)[0]));

        for (String line : lines) {

            String[] values = line.split(sep);
            try {
                ts.setObservation(new Observation(Long.parseLong(values[0]), Double.parseDouble(values[1])));
            }
            catch (Exception e) {

                LOG.error(e.getMessage());
            }
        }
        return ts;
    }

    public void toCSV(String fileName, TimeSeries ts, String header, String sep) throws IOException {

        File file = new File(fileName);
        if (!file.exists()) file.createNewFile();

        FileWriter fw = new FileWriter(fileName);
        fw.write(header + "\n");
        for (Observation observation : ts.observations) {

            fw.write(observation.timestamp + sep + observation.value + "\n");
        }
        fw.close();
    }
}
