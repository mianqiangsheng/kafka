package com.lizhen.listener;

import com.lizhen.entity.DelayMessage;
import com.lizhen.utils.JsonUtils;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;
import java.util.concurrent.DelayQueue;

/**
 * @author ：勉强生
 * @description:
 * @date ：2021/10/8 14:00
 */
@Component
@Log4j2
public class KafkaMultiConsumerListener implements BatchMessageListener<String, String> {

    @Override
    public void onMessage(List<ConsumerRecord<String, String>> data) {

        System.out.println(data);

    }

    @Override
    @KafkaListener(topics = "kafka-topic2", containerFactory = "containerFactory", groupId = "testGroup")
    public void onMessage(List<ConsumerRecord<String, String>> data, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
        this.onMessage(data);
        throw new RuntimeException("消息异常，进入死信队列...");
    }

}
