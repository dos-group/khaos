package de.tu_berlin.dos.arm.khaos.common.api_clients.flink;

import de.tu_berlin.dos.arm.khaos.common.api_clients.flink.responses.StartJob;
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

        String baseUrl = "http://130.149.249.40:30403/";
        Retrofit retrofit =
            new Retrofit.Builder()
                .baseUrl(baseUrl)
                .addConverterFactory(GsonConverterFactory.create())
                .build();
        FlinkRest service = retrofit.create(FlinkRest.class);

        String id = "51f7850e-d8a1-433e-a26f-af9791531050_processor-1.0-SNAPSHOT.jar";
        String programArg = "vehicles,130.149.249.40:32690,iot-vehicles-events-test,iot-vehicles-notifications-test,1,30000";
        int parallelism = 1;

        List<String> jobids = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);
        Call<StartJob> call = service.startJob(id, programArg, parallelism);
        call.enqueue(new Callback<>() {

            @Override
            public void onResponse(Call<StartJob> call, Response<StartJob> response) {

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
