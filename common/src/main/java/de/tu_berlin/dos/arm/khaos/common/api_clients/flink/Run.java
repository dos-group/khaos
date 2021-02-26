package de.tu_berlin.dos.arm.khaos.common.api_clients.flink;

import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.GetTaskmanagers;
import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.StartJob;
import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.Taskmanager;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class Run {

    public static void main(String[] args) throws Exception {
        /* start job example */
        String baseUrl = "http://192.168.64.134:31924/";
        Retrofit retrofit =
            new Retrofit.Builder()
                .baseUrl(baseUrl)
                .addConverterFactory(GsonConverterFactory.create())
                .build();
        FlinkRest service = retrofit.create(FlinkRest.class);

        String id = "a777ab71-e6cd-4b3f-9b24-40a492c7b45b_processor-1.0-SNAPSHOT.jar";
        String programArg = "advertising,130.149.249.40:30190,ad-events,ad-events,1,30000";
        int parallelism = 1;

        List<String> jobids = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Call<StartJob> call = service.startJob(id, programArg, parallelism);
        call.enqueue(new Callback<>() {

            @Override
            public void onResponse(Call<StartJob> call, Response<StartJob> response) {

                System.out.println(response.body());

                assert response.body() != null;
                jobids.add(response.body().jobid);
                latch.countDown();
            }

            @Override
            public void onFailure(Call<StartJob> call, Throwable throwable) {

                throw new IllegalStateException(throwable);
            }
        });
        latch.await();
        System.out.println(Arrays.toString(jobids.toArray()));

        Thread.sleep(10000);
        CountDownLatch latch1 = new CountDownLatch(1);
        /* end start job example */

        /* start get taskmanagers example */
        /*
        String baseUrl = "http://192.168.64.134:31924/";
        Retrofit retrofit =
                new Retrofit.Builder()
                        .baseUrl(baseUrl)
                        .addConverterFactory(GsonConverterFactory.create())
                        .build();
        FlinkRest service = retrofit.create(FlinkRest.class);

        String jobId = "e53bdcd1fe6ce76923d686dacc299dcc";
        String vertexId = "ea632d67b7d595e5b851708ae9ad79d6";

        List<List<Taskmanager>> taskmanagersList = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Call<GetTaskmanagers> call = service.getTaskmanagers(jobId, vertexId);
        call.enqueue(new Callback<>() {

            @Override
            public void onResponse(Call<GetTaskmanagers> call, Response<GetTaskmanagers> response) {
                assert response.body() != null;
                taskmanagersList.add(response.body().taskmanagers);
                latch.countDown();
            }

            @Override
            public void onFailure(Call<GetTaskmanagers> call, Throwable throwable) {
                throw new IllegalStateException(throwable);
            }
        });
        latch.await();

        for (List<Taskmanager> taskmanagers: taskmanagersList) {
            for (Taskmanager taskmanager: taskmanagers) {
                System.out.println(taskmanager.taskmanagerId);
            }
        }

        Thread.sleep(10000);
        CountDownLatch latch1 = new CountDownLatch(1);
        */
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
