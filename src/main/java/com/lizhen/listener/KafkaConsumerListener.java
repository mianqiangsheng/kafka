package com.lizhen.listener;

import com.lizhen.entity.DelayMessage;
import com.lizhen.utils.JsonUtils;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.DelayQueue;

/**
 * 延时队列、死信队列应用
 *
 * @author ：勉强生
 * @description:
 * @date ：2021/10/8 14:00
 */
//@Component
@Log4j2
public class KafkaConsumerListener {

    @Autowired
    private ConsumerFactory consumerFactory;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    static DelayQueue<DelayMessage> delayQueue = new DelayQueue<>();

    /**
     * 设置kafka连接，这里配置重发次数为2次
     * @return
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory containerFactory() {
        ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        //配置批量拉取
        factory.setBatchListener(false);
        // 单条消息消费 最大重试次数2次
//        factory.setErrorHandler(new SeekToCurrentErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate), new FixedBackOff(0L, 2)));

        return factory;
    }

    /**
     * 正常消费kafka消息
     * @param message
     * @param acknowledgment
     */
    @KafkaListener(topics = "kafka-topic1")
    public void onMessage1(String message,Acknowledgment acknowledgment) {
        System.out.println(message);
        acknowledgment.acknowledge();
        log.info("kafka-topic1接收结果:{}", message);
    }

    /**
     * 模拟产生kafka-topic2的死信消息
     * @param record
     * @param acknowledgment
     * @throws Exception
     */
    @KafkaListener(topics = "kafka-topic2", containerFactory = "containerFactory", groupId = "testGroup")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) throws Exception {
        Optional kafkaMessage = Optional.ofNullable(record.value());

        if (!kafkaMessage.isPresent()) {
            throw new Exception("监听到的消息为空值");
        }

        log.info("topicID: " + record.topic());
        log.info("recordValue: " + record.value());

        try {
            /*业务逻辑*/
            throw new RuntimeException("消息异常，进入死信队列...");
        } catch (Exception e) {
            //这里只是本地更新offset，如果异常导致kafka重发消息并没有提交保存该offset。所以重启服务不能获取偏移量信息，这一行代码可有可无
            acknowledgment.acknowledge();
            throw new Exception(e);
        }
    }

    /**
     * 监听kafka-topic2的相关的死信队列消息
     * @param record
     * @param acknowledgment
     * @param exception
     * @param stacktrace
     */
    @KafkaListener(id = "testGroup", topics = "kafka-topic2.DLT", groupId = "testGroup")
    public void dltListen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment,
                          @Header(KafkaHeaders.DLT_EXCEPTION_MESSAGE) String exception,
                          @Header(KafkaHeaders.DLT_EXCEPTION_STACKTRACE) String stacktrace) {
        log.info("Received from DLT: " + record.value());
        acknowledgment.acknowledge();
    }

    /**
     * 拒绝确认消息
     * @param message
     * @param acknowledgment
     */
    @KafkaListener(topics = "kafka-topic3", containerFactory = "containerFactory")
    public void onMessage3(String message,Acknowledgment acknowledgment) {
        System.out.println(message);
        acknowledgment.nack(Duration.ofMillis(0)); //拒绝确认消费消息，会一直从kafka中拉取同一条消息消费，且设置的containerFactory对消费者不提交offset的情况不生效
//        acknowledgment.acknowledge();
        log.info("kafka-topic3接收结果:{}", message);
    }


    /**
     * 延迟队列消费，针对已经可以消费的消息直接发送到真实的业务topic进行消费，针对尚未可以消费的消息放入DelayQueue延迟队列配合DelayQueueMonitor延迟处理
     * （DelayQueueMonitor可以将到期的消息发到死信队列中，在监听死信队列的逻辑里进行对其进行消费）
     * @param json
     * @param acknowledgment
     * @return
     * @throws Throwable
     */
    @KafkaListener(topics = "KAFKA_TOPIC_MESSAGE_DELAY")
    public boolean onMessage2(String json, Acknowledgment acknowledgment) throws Throwable {
        try {
            acknowledgment.acknowledge();
            DelayMessage delayMessage = JsonUtils.toObject(json, JsonUtils.createCollectionType(DelayMessage.class,DelayMessage.class));
            if (!delayMessage.isDelay(delayMessage)) {
                // 如果接收到消息时，消息已经可以发送了，直接发送到实际的队列
                sendActualTopic(delayMessage, json);
            } else {
                // 存储
                localStorage(delayMessage, json);
            }
        } catch (Throwable e) {
            log.error("consumer kafka delay message[{}] error!", json, e);
            throw e;
        }
        return true;
    }

    /**
     * KAFKA_TOPIC_MESSAGE_DELAY中可以消费的消息实际的topic消费
     * @param message
     */
    @KafkaListener(topics = "KAFKA_TOPIC_MESSAGE_DELAY_REAL")
    public void onMessage4(String message){
        System.out.println(message);
        log.info("延迟消息接收成功，KAFKA_TOPIC_MESSAGE_DELAY_REAL接收结果:{}", message);
    }

    private void sendActualTopic(DelayMessage delayMessage, String message) {
        kafkaTemplate.send(delayMessage.getActualTopic(), message);
    }

    private void localStorage(DelayMessage delayMessage, String message) {
//        String key = generateRdbKey(delayMessage);
//        if (rocksDb.keyMayExist(RocksDbUtils.toByte(key), null)) {
//            return;
//        }
//        rocksDb.put(RocksDbUtils.toByte(key), RocksDbUtils.toByte(message));

        delayQueue.put(delayMessage);

    }

    private String generateRdbKey(DelayMessage delayMessage) {
        return delayMessage.getEventTime().getTime() + "_" + delayMessage.getEventId();
    }


}
