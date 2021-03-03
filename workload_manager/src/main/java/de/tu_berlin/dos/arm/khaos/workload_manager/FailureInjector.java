package de.tu_berlin.dos.arm.khaos.workload_manager;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.*;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.fabric8.kubernetes.client.utils.HttpClientUtils;
import okhttp3.OkHttpClient;
import okhttp3.Response;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FailureInjector {

    private final OkHttpClient okHttpClient;
    public final KubernetesClient client;

    public FailureInjector() {
        Config config = new ConfigBuilder().build();
        this.okHttpClient = HttpClientUtils.createHttpClient(config);
        this.client = new DefaultKubernetesClient(okHttpClient, config);
    }

    public String execCommandOnPod(String podName, String namespace, String... cmd) throws InterruptedException, ExecutionException, TimeoutException {
        Pod pod = client.pods().inNamespace(namespace).withName(podName).get();
        System.out.printf("Running command: [%s] on pod [%s] in namespace [%s]%n",
                Arrays.toString(cmd), pod.getMetadata().getName(), namespace);

        CompletableFuture<String> data = new CompletableFuture<>();
        try (ExecWatch execWatch = execCmd(pod, data, cmd)) {
            return data.get(10, TimeUnit.SECONDS);
        }
    }

    private ExecWatch execCmd(Pod pod, CompletableFuture<String> data, String... command) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        return client.pods()
                .inNamespace(pod.getMetadata().getNamespace())
                .withName(pod.getMetadata().getName())
                .writingOutput(baos)
                .writingError(baos)
                .usingListener(new Listener(data, baos))
                .exec(command);
    }

    static class Listener implements ExecListener {

        private CompletableFuture<String> data;
        private ByteArrayOutputStream baos;

        public Listener(CompletableFuture<String> data, ByteArrayOutputStream baos) {
            this.data = data;
            this.baos = baos;
        }

        @Override
        public void onOpen(Response response) {
            //System.out.println("Reading data... " + response.message());
        }

        @Override
        public void onFailure(Throwable t, Response response) {
            //System.err.println(t.getMessage() + " " + response.message());
            data.completeExceptionally(t);
        }

        @Override
        public void onClose(int code, String reason) {
            // System.out.println("Exit with: " + code + " and with reason: " + reason);
            data.complete(baos.toString());
        }
    }

    public void crashFailure(String podName, String namespace) throws InterruptedException, ExecutionException, TimeoutException {

        // kill container
        execCommandOnPod(podName, namespace, "sh", "-c", "kill 1");

        // delete pod
        // client.pods().inNamespace(namespace).withName(podName).delete();

    }

    public static void main(String[] args) throws Exception {

        final String namespace = "default";
        final String podName = "flink-native-taskmanager-1-11";

        // inject crash failure
        FailureInjector failureInjector = new FailureInjector();
        failureInjector.crashFailure(podName, namespace);

        failureInjector.client.close();

    }
}