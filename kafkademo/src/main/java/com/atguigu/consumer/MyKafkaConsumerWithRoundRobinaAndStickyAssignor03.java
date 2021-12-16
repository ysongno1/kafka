package com.atguigu.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class MyKafkaConsumerWithRoundRobinaAndStickyAssignor03 {
    public static void main(String[] args) {
        Properties pros = new Properties();
        //配置集群地址的配置
        pros.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //配置key value 反序列化器
        pros.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        pros.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //配置我们消费者组的信息
        pros.put(ConsumerConfig.GROUP_ID_CONFIG,"test-round-stick-01");
        //配置消费者分区策略  为轮询
        //添加粘性分区策略
        ArrayList<String> strings = new ArrayList<>();
        strings.add("org.apache.kafka.clients.consumer.RoundRobinAssignor");
        strings.add("org.apache.kafka.clients.consumer.StickyAssignor");
        pros.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, strings);


        // 创建一个kafka 消费者
        KafkaConsumer<String, String> myKafkaConsumer = new KafkaConsumer<String, String>(pros);
        //创建一个放置topic 的集合
        ArrayList<String> topics = new ArrayList<>();
        topics.add("four");

        myKafkaConsumer.subscribe( topics);//订阅一批topic
        while(true){//使用while(true)使我们的消费者一直去拉取消息
            //拉取kafka的消息
            ConsumerRecords<String, String> myKafkaRecoords = myKafkaConsumer.poll(Duration.ofSeconds(2));
            //可以对我们消费到的信息进行遍历读取
            for (ConsumerRecord<String, String> myKafkaRecoord : myKafkaRecoords) {
                //打印我们的消费到的消息
                System.out.printf("offset =  %s ,topic = %s, partition = %s value = %s \n",myKafkaRecoord.offset(),
                        myKafkaRecoord.topic(),myKafkaRecoord.partition(),myKafkaRecoord.value());
            }

        }




    }
}
