package com.lizhen.listener;

import com.lizhen.utils.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author ：li zhen
 * @description:
 * @date ：2024/1/22 10:43
 */
@Component
@Slf4j
public class SequenceConsumerListener implements BatchMessageListener<String, String> {

    //初始化线程池，这里设置为3个线程异步消费执行业务逻辑
    ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(3);

    //初始化3个task，匹配线程池的线程数，将每个partition的消息交给3个异步线程执行进行消费
    OperationTask task1 = new OperationTask("taskList1");
    OperationTask task2 = new OperationTask("taskList2");
    OperationTask task3 = new OperationTask("taskList3");

    @Override
    public void onMessage(List<ConsumerRecord<String, String>> data) {

        List<Task> tasks = data.stream().map(d -> {
            //如果顺序粒度保持到key，可以把key传入Task中，用key来作为分配依据
            String key = d.key();
            String value = d.value();
            Task task = JsonUtils.toObject(value, Task.class);
            return task;
        }).collect(Collectors.toList());

//        //初始化3个task，匹配线程池的线程数，将该partition的消息交给3个异步线程执行进行消费
//        OperationTask task1 = new OperationTask("taskList1");
//        OperationTask task2 = new OperationTask("taskList2");
//        OperationTask task3 = new OperationTask("taskList3");

        //分配消息到3个任务线程
        dispatchThread(task1, task2, task3, tasks);

        //将3个task交给线程池执行，每个task按照一定规则将从partition读取的消息有序放入，按顺序执行
        executorService.execute(task1);
        executorService.execute(task2);
        executorService.execute(task3);

        //等待所有任务完成
        boolean loop = true;
        do {
            try {
                loop = !executorService.awaitTermination(2, TimeUnit.SECONDS);  //阻塞，直到线程池里所有任务结束
            } catch (InterruptedException e) {
                log.error(e.getMessage());
            }
        } while (loop);



    }

    //指定消费kafka-topic2下的1分区下的消息
    @Override
    @KafkaListener(topics = "kafka-topic2", topicPartitions = {@TopicPartition(topic = "kafka-topic2", partitions = {"1"})}, containerFactory = "containerFactory", groupId = "testGroup")
    public void onMessage(List<ConsumerRecord<String, String>> data, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
        this.onMessage(data);

        //提交此次批次的offset
        Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(
                new org.apache.kafka.common.TopicPartition("kafka-topic2", 1),
                new OffsetAndMetadata(data.get(data.size() - 1).offset() + 1));
        consumer.commitSync(offsets);
//        acknowledgment.acknowledge();
    }

    /**
     * 分配partition中的消息给3个线程
     *
     * @param task1
     * @param task2
     * @param task3
     * @param partition
     */
    private static void dispatchThread(OperationTask task1, OperationTask task2, OperationTask task3, List<Task> partition) {
        for (Task s : partition) {
            //使用消息的id进行取模运算
            int mod = Math.floorMod(s.id, 3);
            switch (mod) {
                case 0:
                    task1.addTask(s);
                    break;
                case 1:
                    task2.addTask(s);
                    break;
                case 2:
                    task3.addTask(s);
                    break;
                default:
                    task1.addTask(s);
                    break;
            }
        }
    }

    static class OperationTask implements Runnable {

        public OperationTask(String name) {
            this.name = name;
        }

        private final String name;
        //配置下发任务队列
        private final LinkedList<Task> tasks = new LinkedList<>();

        //添加配置下发任务
        public void addTask(Task task) {
            this.tasks.add(task);
        }

        @Override
        public void run() {
            //执行配置下发操作
            System.out.println(name + " start.");
            int count = 0;
            while (!tasks.isEmpty()) {
                System.out.println("TaskOperator: " + name + " operate task : " + tasks.pop());
                try {
                    //执行业务逻辑
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
                count++;
            }

            System.out.println(name + " end with " + count + " completed");
        }
    }

    static class Task {
        public Integer id;
        public String name;

        public Task(Integer id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String toString() {
            return "Task{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
    }
}
