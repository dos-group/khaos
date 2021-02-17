package de.tu_berlin.dos.arm.khaos.workload_manager;

import de.tu_berlin.dos.arm.khaos.common.prometheus.models.VectorResponse;
import org.apache.log4j.Logger;

public class Run {

    private static final Logger LOG = Logger.getLogger(Run.class);

    public static void main(String[] args) throws Exception {

        ExecutionGraph.start();

        //String query = String.format("sum(%s{job_name='%s'})", Context.GET.metricsThroughput, Context.GET.flinkJobName);
        //VectorResponse response = Context.GET.prometheusClient.query(query);
        //System.out.println(response.data.result);


    }
}
