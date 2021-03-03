package de.tu_berlin.dos.arm.khaos.common.api_clients.flink;

import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.Checkpoints;
import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.Vertices;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Run {

    public static void main(String[] args) throws Exception {
        /* start job example */
        /*
        String baseUrl = "http://130.149.249.40:32038//";
        Retrofit retrofit =
            new Retrofit.Builder()
                .baseUrl(baseUrl)
                .addConverterFactory(GsonConverterFactory.create())
                .build();
        FlinkRest service = retrofit.create(FlinkRest.class);

        String id = "efcc2dae-a173-40b7-a18d-07609d31967f_processor-1.0-SNAPSHOT.jar";
        String programArg = "vehicles-test,130.149.249.40:32690,iot-vehicles-events-test,iot-vehicles-notifications-test,1,30000";
        int parallelism = 1;

        String jobId = "e87033bf22c8a0c7341d48a1ce6a74d9";
        List<String> jobIds = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Call<Job> call = service.startJob(id, programArg, parallelism);
        call.enqueue(new Callback<>() {

            @Override
            public void onResponse(Call<Job> call, Response<Job> response) {

                System.out.println(response.body());

                assert response.body() != null;
                jobIds.add(response.body().jobId);
                latch.countDown();
            }

            @Override
            public void onFailure(Call<Job> call, Throwable throwable) {

                throw new IllegalStateException(throwable);
            }
        });
        latch.await();
        System.out.println(Arrays.toString(jobIds.toArray()));

        //Thread.sleep(10000);
        CountDownLatch latch1 = new CountDownLatch(1);
        */
        /* end start job example */

        /* start get vertices example */
        /*
        String baseUrl = "http://192.168.64.136:32260/";
        Retrofit retrofit =
                new Retrofit.Builder()
                        .baseUrl(baseUrl)
                        .addConverterFactory(GsonConverterFactory.create())
                        .build();
        FlinkRest service = retrofit.create(FlinkRest.class);

        String jobId = "2fbaf2a154541e0482081c7f21360a6b";

        List<List<Vertices.Node>> operatorIds = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Call<Vertices> call = service.getVertices(jobId);
        call.enqueue(new Callback<>() {

            @Override
            public void onResponse(Call<Vertices> call, Response<Vertices> response) {
                assert response.body() != null;
                operatorIds.add(response.body().plan.nodes);
                latch.countDown();
            }

            @Override
            public void onFailure(Call<Vertices> call, Throwable throwable) {
                throw new IllegalStateException(throwable);
            }
        });
        latch.await();

        for (List<Vertices.Node> nodes: operatorIds) {
            for (Vertices.Node node : nodes) {
                System.out.println(node.id);
            }
        }

        */
        //Thread.sleep(10000);
        //CountDownLatch latch1 = new CountDownLatch(1);
        /* end get vertices example */

        /* start get checkpoint example */
        String baseUrl = "http://192.168.64.136:32260/";
        Retrofit retrofit =
                new Retrofit.Builder()
                        .baseUrl(baseUrl)
                        .addConverterFactory(GsonConverterFactory.create())
                        .build();
        FlinkRest service = retrofit.create(FlinkRest.class);

        String jobId = "2fbaf2a154541e0482081c7f21360a6b";

        final Checkpoints.Completed[] latestCheckpoint = new Checkpoints.Completed[1];
        CountDownLatch latch = new CountDownLatch(1);
        Call<Checkpoints> call = service.getCheckpoints(jobId);
        call.enqueue(new Callback<>() {

            @Override
            public void onResponse(Call<Checkpoints> call, Response<Checkpoints> response) {
                assert response.body() != null;
                latestCheckpoint[0] = response.body().latest.completed;
                latch.countDown();
            }

            @Override
            public void onFailure(Call<Checkpoints> call, Throwable throwable) {
                throw new IllegalStateException(throwable);
            }
        });
        latch.await();

        // last checkpoint complete status
        System.out.println(latestCheckpoint[0].status);

        // last checkpoint timestamp
        System.out.println(latestCheckpoint[0].latestAckTimestamp);


        //Thread.sleep(10000);
        //CountDownLatch latch1 = new CountDownLatch(1);
        /* end get checkpoint example */




        /* start get taskmanagers example */
        /*String baseUrl = "http://130.149.249.40:32038/";
        Retrofit retrofit =
                new Retrofit.Builder()
                        .baseUrl(baseUrl)
                        .addConverterFactory(GsonConverterFactory.create())
                        .build();
        FlinkRest service = retrofit.create(FlinkRest.class);

        String jobId = "5151103503f8f7c39ec52d1c0679b764";
        String vertexId = "0a448493b4782967b150582570326227";

        List<List<TaskManager>> taskManagersList = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Call<TaskManagers> call = service.getTaskManagers(jobId, vertexId);
        call.enqueue(new Callback<>() {

            @Override
            public void onResponse(Call<TaskManagers> call, Response<TaskManagers> response) {
                assert response.body() != null;
                taskManagersList.add(response.body().taskManagers);
                latch.countDown();
            }

            @Override
            public void onFailure(Call<TaskManagers> call, Throwable throwable) {
                throw new IllegalStateException(throwable);
            }
        });
        latch.await();

        for (List<TaskManager> taskmanagers: taskManagersList) {

            for (TaskManager taskmanager: taskmanagers) {
                System.out.println(taskmanager.taskManagerId);
            }
        }*/

        //Thread.sleep(10000);
        //CountDownLatch latch1 = new CountDownLatch(1);

        /* end get taskmanagers example */







        /*Call<Void> call2 = service.cancelJob(jobids.get(0));
        call2.enqueue(new Callback<>() {

            @Override
            public void onResponse(Call<Void> call, Response<Void> response) {

                latch.countDown();
            }

            @Override
            public void onFailure(Call<Void> call, Throwable throwable) {

                throw new IllegalStateException(throwable);
            }
        });
        latch.await();*/

        System.out.println("done");
    }
}
