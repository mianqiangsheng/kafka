package com.lizhen.listener;

import com.lizhen.entity.DelayMessage;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 处理KAFKA_TOPIC_MESSAGE_DELAY延迟消费消息，只有到期的消息才被处理
 *
 * @author ：勉强生
 * @description:
 * @date ：2021/10/11 12:30
 */
@Component
public class DelayQueueMonitor implements ApplicationRunner {


    @Override
    public void run(ApplicationArguments args) throws Exception {

        System.out.println("订单延迟队列开始时间:" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        while (true){
            while (KafkaConsumerListener.delayQueue.size() > 0) {
                /**
                 * 取队列头部元素是否过期
                 */
                DelayMessage delayMessage = KafkaConsumerListener.delayQueue.poll();
                if (delayMessage != null) {
                    System.out.format("延迟消息被消费:{%s}\n", delayMessage);
                }
                Thread.sleep(1000);
            }
        }

    }
}
