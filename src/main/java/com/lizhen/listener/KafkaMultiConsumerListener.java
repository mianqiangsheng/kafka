package com.lizhen.listener;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.RetryingBatchErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

/**
 * 批量消费应用
 *
 * @author ：勉强生
 * @description:
 * @date ：2021/10/8 14:00
 */
@Component
@Log4j2
public class KafkaMultiConsumerListener implements BatchMessageListener<String, String> {

    @Autowired
    private ConsumerFactory consumerFactory;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    /**
     * 设置kafka连接，这里配置重发次数为2次
     * @return
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory containerFactory() {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        //配置批量拉取消息
        factory.setBatchListener(true);
        //多条消息消费 最大重试次数2次
        factory.setBatchErrorHandler(new RetryingBatchErrorHandler(new FixedBackOff(0L, 2), new DeadLetterPublishingRecoverer(kafkaTemplate)));

        return factory;
    }

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
