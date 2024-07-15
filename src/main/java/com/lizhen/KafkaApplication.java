package com.lizhen;

import com.lizhen.parallel_consumer_application.CoreApp;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.concurrent.CountDownLatch;

/**
 * @author ：勉强生
 * @description:
 * @date ：2021/10/8 13:56
 */
@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) throws InterruptedException {
        SpringApplication.run(KafkaApplication.class, args);

        CountDownLatch countDownLatch = new CountDownLatch(3);
        CoreApp coreApp = new CoreApp(countDownLatch,"input-topic","output-topic");
        coreApp.runBathPollAndProduce();
        countDownLatch.await();
        coreApp.close();
    }
}
