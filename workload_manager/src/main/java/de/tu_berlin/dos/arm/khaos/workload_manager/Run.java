package de.tu_berlin.dos.arm.khaos.workload_manager;

import de.tu_berlin.dos.arm.khaos.common.utils.FileReader;
import de.tu_berlin.dos.arm.khaos.workload_manager.Consumers.Consumer;
import de.tu_berlin.dos.arm.khaos.workload_manager.Consumers.KafkaToFileConsumer;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Map;
import java.util.Properties;

public class Run {

    private static final Logger LOG = Logger.getLogger(Run.class);

    public static void main(String[] args) throws Exception {

        // get properties file
        Properties generatorProps = FileReader.GET.read("generator.properties", Properties.class);

        // retrieve file for outputting events
        String brokerList = generatorProps.getProperty("kafka.brokerList");
        String topic = generatorProps.getProperty("kafka.topic");
        // create output file if it doesnt exist
        String fileName = generatorProps.getProperty("dataset.outputFile");
        File outputFile = new File(fileName);
        if (!outputFile.createNewFile()) throw new IllegalStateException("Unable to crate output file");

        Consumer consumer = new KafkaToFileConsumer(brokerList, topic, 100000, outputFile);
        consumer.execute();




        // get properties file
        //Properties generatorProps = FileReader.GET.read("generator.properties", Properties.class);

        //String inputFileName = generatorProps.getProperty("dataset.inputFile");
        //File inputFile = FileReader.GET.read(inputFileName, File.class);
        //String tsLabel = generatorProps.getProperty("dataset.tsLabel");

        //File outputFile = new File("iot_vehicles_events.csv");

        //sort(inputFile, outputFile, "ts");

    }
}
