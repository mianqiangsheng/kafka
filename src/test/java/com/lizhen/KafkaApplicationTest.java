package com.lizhen;

import com.fasterxml.jackson.databind.JavaType;
import com.lizhen.entity.DelayMessage;
import com.lizhen.utils.JsonUtils;

public class KafkaApplicationTest {

    public static void main(String[] args) {
        final JavaType collectionType = JsonUtils.createCollectionType(DelayMessage.class,DelayMessage.class);
        System.out.println(collectionType);
    }
}