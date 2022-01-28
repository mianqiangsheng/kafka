package com.lizhen.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * @author ：勉强生
 * @description:
 * @date ：2021/10/8 14:01
 */
@Configuration
@ConditionalOnClass(KafkaAdmin.class)
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaBaseConfiguration {

    private final KafkaProperties properties;


    public KafkaBaseConfiguration(KafkaProperties properties) {
        this.properties = properties;
    }

    /**
     * 初始化对kafka执行操作的对象
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        KafkaAdmin admin = new KafkaAdmin(this.properties.buildProducerProperties());
        return admin;
    }

    /**
     * 初始化操作连接
     */
    @Bean
    public AdminClient adminClient() {
        return AdminClient.create(kafkaAdmin().getConfigurationProperties());
    }
}