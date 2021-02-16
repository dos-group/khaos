package de.tu_berlin.dos.arm.khaos.common.utils;

import com.github.davidmoten.bigsorter.*;
import com.google.gson.JsonParser;

import java.io.File;
import java.util.Comparator;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class DatasetSorter {

    public static void sort(String inFile, String tempDir, String outFile, String tsLabel) {

        Comparator<String> byTimestamp = (ts1, ts2) -> {

            try {
                Date tsA = DateUtil.provideDateFormat().parse(JsonParser.parseString(ts1).getAsJsonObject().get(tsLabel).getAsString());
                Date tsB = DateUtil.provideDateFormat().parse(JsonParser.parseString(ts2).getAsJsonObject().get(tsLabel).getAsString());
                return tsA.compareTo(tsB);
            }
            catch (ParseException ex) {
                throw new RuntimeException(ex.getMessage());
            }
        };

        Sorter
            .serializerLinesUtf8()
            .comparator(byTimestamp)
            .input(new File(inFile))
            .output(new File(outFile))
            .tempDirectory(new File(tempDir))
            .maxFilesPerMerge(100)
            .maxItemsPerFile(1000000)
            .loggerStdOut()
            .sort();
    }
}
