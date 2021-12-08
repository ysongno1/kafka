package com.atguigu.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

//分区分配策略
//默认使用Range的分区分配策略，
// 可以通过参数"partition.assignment.strategy"的值进行修改，
// 可以使用多个分区分配策略。

//轮询分区策略RoundRobin测试
//topic:third拥有7个分区

//修改分区分配策略
//props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");

//注意：3个消费者都应该注释掉，之后重启3个消费者，
// 如果出现报错，全部停止等会再重启，或者修改为全新的消费者组

public class MyKafkaConsumerRoundRobin01 {
    public static void main(String[] args) {

        Properties props = new Properties();

        //配置集群
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        //配置key,value的反序列化器
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        //配置消费组信息
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"test");

        //修改分区分配策略
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.RoundRobinAssignor");


        //创建一个kafka的消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(props);

        //订阅一批topic
        kafkaConsumer.subscribe(Collections.singleton("third"));

        //使用while(true)是我们的消费者一直去拉取消息
        while (true) {
            //拉取kafka的消息
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            //可以对我们消费的信息进行遍历读取并打印
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println("offset:" + consumerRecord.offset() +
                        " topic:" + consumerRecord.topic() +
                        " partition:" + consumerRecord.partition() +
                        " value:" + consumerRecord.value());
            }
        }
    }
}
