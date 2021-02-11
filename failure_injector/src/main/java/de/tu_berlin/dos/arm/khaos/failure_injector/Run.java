package de.tu_berlin.dos.arm.khaos.failure_injector;

import de.tu_berlin.dos.arm.khaos.common.utils.FileReader;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class Run {

    private static final Logger logger = LoggerFactory.getLogger(Run.class);

    public static void crashFailure(String targetLabel, String namespace, int targetCount) throws InterruptedException, ExecutionException, TimeoutException {

        // connect to k8s with $HOME/.kube/config file
        final ConfigBuilder configBuilder = new ConfigBuilder();
        try (KubernetesClient client = new DefaultKubernetesClient(configBuilder.build())) {

            // collect list of pods to target
            PodList podList =  client.pods().inNamespace(namespace).withLabel(targetLabel).list();

            // inject target number of pods with failure
            for (int i=0; i<targetCount; i++) {

                String podName = podList.getItems().get(i).getMetadata().getName();

                // delete pods
                client.pods().inNamespace(namespace).withName(podName).delete();

                /*
                // kill container
                try (ExecuteCommandOnPod example = new ExecuteCommandOnPod()) {
                    example.execCommandOnPod(podName, namespace, "sh", "-c", "kill 1");
                }
                */

            }

        } catch (KubernetesClientException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static void main(String[] args) throws Exception {

        Properties failureProps = FileReader.GET.read("failure_injector.properties", Properties.class);
        final String namespace = failureProps.getProperty("kubernetes.namespace");
        final String targetLabel = failureProps.getProperty("target.label");
        final int targetCount = Integer.parseInt(failureProps.getProperty("target.count"));

        // inject crash failure
        crashFailure(targetLabel, namespace, targetCount);

    }
}