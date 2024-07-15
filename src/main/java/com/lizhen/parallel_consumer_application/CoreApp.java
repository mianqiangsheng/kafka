package com.lizhen.parallel_consumer_application;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.confluent.parallelconsumer.RecordContext;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.concurrent.CircuitBreakingException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Basic core examples
 */
@Slf4j
public class CoreApp {

    /**
     * 控制消费几条消息后关闭，非必要
     */
    CountDownLatch countDownLatch;

//    String inputTopic = "input-topic-" + RandomUtils.nextInt();
//    String outputTopic = "output-topic-" + RandomUtils.nextInt();
    String inputTopic = "input-topic";
    String outputTopic = "output-topic";

    public CoreApp(CountDownLatch countDownLatch, String inputTopic, String outputTopic) {
        this.countDownLatch = countDownLatch;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    /**
     * 配置kafka消费者
     * @return
     */
    Consumer<String, String> getKafkaConsumer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "127.0.0.1:9092");
        props.setProperty("group.id", "group-1");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        /**
         * 避免rebalance引起的重复消费
         */
        props.setProperty("max.poll.records", "100");
        props.setProperty("max.poll.interval.ms", "300000");
        props.setProperty("session.timeout.ms", "30000");
        props.setProperty("heartbeat.interval.ms", "8000");

        /**
         * 提高消费者每次拉取的消息数量，减少对kakfa broker发起网络IO请求
         */
        props.setProperty("fetch.max.bytes", "52428800");
        props.setProperty("max.partition.fetch.bytes", "26214400");

        return new KafkaConsumer<>(props);
    }

    /**
     * 配置kafka生产者（如果不需要下发消息则无需配置）
     * @return
     */
    Producer<String, String> getKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092"); // Kafka集群地址
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 100); // 批次大小
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 确认模式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"); // 键序列化器
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"); // 值序列化器
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5); // 单个连接最多未被确认的消息数
        props.put(ProducerConfig.RETRIES_CONFIG, 3); // 重试次数
//        props.put(ProducerConfig.LINGER_MS_CONFIG, 1); // 发送延迟
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 记录缓冲区大小

        return new KafkaProducer<>(props);
    }

    ParallelStreamProcessor<String, String> parallelConsumer;

    @SuppressWarnings("UnqualifiedFieldAccess")
    void run() {
        this.parallelConsumer = setupParallelConsumer(false);

        postSetup();

        // tag::example[]
        parallelConsumer.poll(record ->
                log.info("Concurrently processing a record: {}", record)
        );
        // end::example[]
    }

    protected void postSetup() {
        // ignore
    }

    /**
     * 配置消息并发处理器
     * @param batch
     * @return
     */
    @SuppressWarnings({"FeatureEnvy", "MagicNumber"})
    ParallelStreamProcessor<String, String> setupParallelConsumer(Boolean batch) {
        // tag::exampleSetup[]
        Consumer<String, String> kafkaConsumer = getKafkaConsumer(); // <1>
        Producer<String, String> kafkaProducer = getKafkaProducer();

        var options = ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS) //是否提交offset的时候阻塞停止继续消费，如果同步提交则保证提交的offset更接近实际处理过的消息位置，如果异步提交则可能提交的offset离实际处理过的消息位置更远，这两种都可能重复消息，都要保证消费幂等
                .ordering(UNORDERED) // <2> 提供3种级别的并发顺序保证，1：不要求顺序的 2：partition级别的顺序 3：自定义key级别的顺序
                .maxConcurrency(10) // <3>
                .batchSize(batch?50:1) // <5> 是否批量处理消息，如果业务处理可以批量，则进行批量处理能增加消费速度，但是如果一批数据中有一条出现错误则会导致该批次所有数据重新返回线程的待处理消息阻塞队列，增大了消息处理规模维度
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build();

        ParallelStreamProcessor<String, String> eosStreamProcessor =
                ParallelStreamProcessor.createEosStreamProcessor(options);

        eosStreamProcessor.subscribe(of(inputTopic)); // <4>

        return eosStreamProcessor;
        // end::exampleSetup[]
    }

    public void close() {
        this.parallelConsumer.close();
    }

    /**
     * 并发消费并转发到下游
     */
    public void runPollAndProduce() {
        this.parallelConsumer = setupParallelConsumer(false);

        postSetup();

        // tag::exampleProduce[]
        parallelConsumer.pollAndProduce(context -> {
                    var consumerRecord = context.getSingleRecord().getConsumerRecord();
                    var result = processBrokerRecord(consumerRecord);
                    return new ProducerRecord<>(outputTopic, consumerRecord.key(), result.payload);
                }, consumeProduceResult -> {
                    log.debug("Message {} saved to broker at offset {}",
                            consumeProduceResult.getOut(),
                            consumeProduceResult.getMeta().offset());
                }
        );
        // end::exampleProduce[]
    }

    /**
     * 批量并发消费并转发到下游
     */
    public void runBathPollAndProduce() {
        this.parallelConsumer = setupParallelConsumer(true);

        postSetup();

        // tag::exampleProduce[]
        parallelConsumer.pollAndProduceMany(context -> {
                    // convert the batch into the payload for our processing
                    List<ProducerRecord<String, String>> payloads = context.stream()
                            .map(this::preparePayload1)
                            .collect(Collectors.toList());
                    // process the entire batch payload at once
                    processBatchPayload1(payloads);
                    return payloads;
                    }, consumeProduceResult -> {
                    log.debug("Message {} saved to broker at offset {}",
                            consumeProduceResult.getOut(),
                            consumeProduceResult.getMeta().offset());
                    if (countDownLatch != null){
                        countDownLatch.countDown();
                    }
        }
        );
        // end::exampleProduce[]
    }

    private Result processBrokerRecord(ConsumerRecord<String, String> consumerRecord) {
        return new Result("Some payload from " + consumerRecord.value());
    }

    @Value
    static class Result {
        String payload;
    }

    void customRetryDelay() {
        // tag::customRetryDelay[]
        final double multiplier = 0.5;
        final int baseDelaySecond = 1;

        ParallelConsumerOptions.<String, String>builder()
                .retryDelayProvider(recordContext -> {
                    int numberOfFailedAttempts = recordContext.getNumberOfFailedAttempts();
                    long delayMillis = (long) (baseDelaySecond * Math.pow(multiplier, numberOfFailedAttempts) * 1000);
                    return Duration.ofMillis(delayMillis);
                });
        // end::customRetryDelay[]
    }


    void maxRetries() {
        ParallelStreamProcessor<String, String> pc = ParallelStreamProcessor.createEosStreamProcessor(null);
        // tag::maxRetries[]
        final int maxRetries = 10;
        final Map<ConsumerRecord<String, String>, Long> retriesCount = new ConcurrentHashMap<>();

        pc.poll(context -> {
            var consumerRecord = context.getSingleRecord().getConsumerRecord();
            Long retryCount = retriesCount.compute(consumerRecord, (key, oldValue) -> oldValue == null ? 0L : oldValue + 1);
            if (retryCount < maxRetries) {
                processRecord(consumerRecord);
                // no exception, so completed - remove from map
                retriesCount.remove(consumerRecord);
            } else {
                log.warn("Retry count {} exceeded max of {} for record {}", retryCount, maxRetries, consumerRecord);
                // giving up, remove from map
                retriesCount.remove(consumerRecord);
            }
        });
        // end::maxRetries[]
    }

    private void processRecord(final ConsumerRecord<String, String> record) {
        // no-op
    }

    void circuitBreaker() {
        ParallelStreamProcessor<String, String> pc = ParallelStreamProcessor.createEosStreamProcessor(null);
        // tag::circuitBreaker[]
        final Map<String, Boolean> upMap = new ConcurrentHashMap<>();

        pc.poll(context -> {
            var consumerRecord = context.getSingleRecord().getConsumerRecord();
            String serverId = extractServerId(consumerRecord);
            boolean up = upMap.computeIfAbsent(serverId, ignore -> true);

            if (!up) {
                up = updateStatusOfSever(serverId);
            }

            if (up) {
                try {
                    processRecord(consumerRecord);
                } catch (CircuitBreakingException e) {
                    log.warn("Server {} is circuitBroken, will retry message when server is up. Record: {}", serverId, consumerRecord);
                    upMap.put(serverId, false);
                }
                // no exception, so set server status UP
                upMap.put(serverId, true);
            } else {
                throw new RuntimeException(msg("Server {} currently down, will retry record latter {}", up, consumerRecord));
            }
        });
        // end::circuitBreaker[]
    }

    private boolean updateStatusOfSever(final String serverId) {
        return false;
    }

    private String extractServerId(final ConsumerRecord<String, String> consumerRecord) {
        // no-op
        return null;
    }


    void batching() {
        // tag::batching[]
        ParallelStreamProcessor.createEosStreamProcessor(ParallelConsumerOptions.<String, String>builder()
                .consumer(getKafkaConsumer())
                .producer(getKafkaProducer())
                .maxConcurrency(100)
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .batchSize(5) // <1>
                .build());
        parallelConsumer.poll(context -> {
            // convert the batch into the payload for our processing
            List<String> payload = context.stream()
                    .map(this::preparePayload)
                    .collect(Collectors.toList());
            // process the entire batch payload at once
            processBatchPayload(payload);
        });
        // end::batching[]
    }

    private void processBatchPayload(List<String> batchPayload) {
        // example
    }

    private void processBatchPayload1(List<ProducerRecord<String, String>> batchPayload) {
        System.out.println(Arrays.toString(batchPayload.toArray()));
    }

    private String preparePayload(RecordContext<String, String> rc) {
        ConsumerRecord<String, String> consumerRecords = rc.getConsumerRecord();
        int failureCount = rc.getNumberOfFailedAttempts();
        return msg("{}, {}", consumerRecords, failureCount);
    }


    private ProducerRecord<String, String> preparePayload1(RecordContext<String, String> rc) {
        ConsumerRecord<String, String> consumerRecords = rc.getConsumerRecord();
        return new ProducerRecord<>(outputTopic, consumerRecords.key(), consumerRecords.value());
    }

}
