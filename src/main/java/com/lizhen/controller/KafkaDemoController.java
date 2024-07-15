package com.lizhen.controller;

import com.lizhen.entity.DeviceDataReceive;
import com.lizhen.entity.EnergyItemAggregate;
import com.lizhen.utils.JsonUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author ：勉强生
 * @description:
 * @date ：2021/10/8 14:00
 */
@RestController
public class KafkaDemoController {

    @Autowired
    AdminClient adminClient;

    /**
     * 创建topic
     */
    @RequestMapping("createTopic")
    public String createTopic(String topicName) {
        NewTopic topic = new NewTopic(topicName, 2, (short) 1);
        adminClient.createTopics(Arrays.asList(topic));
        return topicName;
    }

    /**
     * 查询topic
     */
    @RequestMapping("queryTopic")
    public String queryTopic(String topicName) {
        DescribeTopicsResult result = adminClient.describeTopics(Arrays.asList(topicName));
        StringBuffer sb = new StringBuffer("topic信息:");
        try {
            result.all().get().forEach((k, v) -> sb.append("key").append(k).append(";v:").append(v));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    /**
     * 删除topic
     */
    @RequestMapping("deleteTopic")
    public String deleteTopic(String topicName) {
        adminClient.deleteTopics(Arrays.asList(topicName));
        return topicName;
    }

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * 发送文字消息
     */
    @RequestMapping("sendStr")
    public String sendStr(String message) {
        kafkaTemplate.send("kafka-topic1", message);
        return message;
    }

    @RequestMapping("sendStr2")
    public String sendStr2(String message) {
        kafkaTemplate.send("kafka-topic2", message);
        return message;
    }

    @RequestMapping("/object/send")
    public String sendStr3(@RequestBody EnergyItemAggregate energyItemAggregate) {
        String message = JsonUtils.toJson(energyItemAggregate);
        kafkaTemplate.send("kafka-topic2", message);
        return message;
    }

    @RequestMapping("sendStr3")
    public String sendStr3(String message) {
        kafkaTemplate.send("kafka-topic3", message);
        return message;
    }

    @PostMapping("sendDelayMsg")
    public String sendDelayMsg(@RequestBody String message) {
        kafkaTemplate.send("KAFKA_TOPIC_MESSAGE_DELAY", message);
        return message;
    }

    @RequestMapping("sequential/consume")
    public String sequentialConsume(String message) {
        kafkaTemplate.send("TOPIC_INSERT", message);
        kafkaTemplate.send("TOPIC_UPDATE", message);
        return message;
    }


    @RequestMapping("/parallel/send")
    public String sendStr3() {

        String message1 = "{\"buildingId\":\"1\",\"meters\":[{\"meterId\":\"A\",\"dateTime\":\"2024-07-10 01:00:00\",\"parameters\":[{\"parameter\":\"WPP\",\"value\":10},{\"parameter\":\"Ua\",\"value\":1},{\"parameter\":\"Ib\",\"value\":2}]},{\"meterId\":\"B\",\"dateTime\":\"2024-07-10 01:00:00\",\"parameters\":[{\"parameter\":\"WPP\",\"value\":20},{\"parameter\":\"Ua\",\"value\":3},{\"parameter\":\"Ib\",\"value\":4}]}]}";
        String message2 = "{\"buildingId\":\"2\",\"meters\":[{\"meterId\":\"A\",\"dateTime\":\"2024-07-10 01:00:00\",\"parameters\":[{\"parameter\":\"WPP\",\"value\":10},{\"parameter\":\"Ua\",\"value\":1},{\"parameter\":\"Ib\",\"value\":2}]},{\"meterId\":\"B\",\"dateTime\":\"2024-07-10 01:00:00\",\"parameters\":[{\"parameter\":\"WPP\",\"value\":20},{\"parameter\":\"Ua\",\"value\":3},{\"parameter\":\"Ib\",\"value\":4}]}]}";
        String message3 = "{\"buildingId\":\"1\",\"meters\":[{\"meterId\":\"A\",\"dateTime\":\"2024-07-10 02:00:00\",\"parameters\":[{\"parameter\":\"WPP\",\"value\":10},{\"parameter\":\"Ua\",\"value\":1},{\"parameter\":\"Ib\",\"value\":2}]},{\"meterId\":\"B\",\"dateTime\":\"2024-07-10 02:00:00\",\"parameters\":[{\"parameter\":\"WPP\",\"value\":20},{\"parameter\":\"Ua\",\"value\":3},{\"parameter\":\"Ib\",\"value\":4}]}]}";
        String message4 = "{\"buildingId\":\"2\",\"meters\":[{\"meterId\":\"A\",\"dateTime\":\"2024-07-10 02:00:00\",\"parameters\":[{\"parameter\":\"WPP\",\"value\":10},{\"parameter\":\"Ua\",\"value\":1},{\"parameter\":\"Ib\",\"value\":2}]},{\"meterId\":\"B\",\"dateTime\":\"2024-07-10 02:00:00\",\"parameters\":[{\"parameter\":\"WPP\",\"value\":20},{\"parameter\":\"Ua\",\"value\":3},{\"parameter\":\"Ib\",\"value\":4}]}]}";

        List<String> messages = Arrays.asList(message1, message2, message3, message4);

        for (String message : messages) {
            DeviceDataReceive deviceDataReceive = JsonUtils.toObject(message, DeviceDataReceive.class);
            kafkaTemplate.send("input-topic", deviceDataReceive.getBuildingId(),message);
        }

        return "success";
    }
}
