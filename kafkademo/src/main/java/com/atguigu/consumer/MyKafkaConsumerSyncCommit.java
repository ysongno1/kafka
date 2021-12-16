package com.atguigu.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

/*
    虽然自动提交offset十分简介便利，但由于其是基于时间提交的，开发人员难以把握offset提交的时机。
    因此Kafka还提供了手动提交offset的API。
    手动提交offset的方法有两种：分别是commitSync（同步提交）和commitAsync（异步提交）。
        两者的相同点是，都会将本次poll的一批数据最高的偏移量提交；
        不同点是，commitSync阻塞当前线程，一直到提交成功，并且会自动失败重试（由不可控因素导致，也会出现提交失败）；
                而commitAsync则没有失败重试机制，故有可能提交失败。
 */

public class MyKafkaConsumerSyncCommit {
    public static void main(String[] args) {
        Properties pros = new Properties();
        //配置集群地址的配置
        pros.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        //配置key value 反序列化器
        pros.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        pros.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        //配置我们消费者组的信息
        pros.put(ConsumerConfig.GROUP_ID_CONFIG,"test");


        // 是否自动提交offset
        pros.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 提交offset的时间周期
        //当自动提交设置为哦false的时候 提交offset的时间周期 参数 没有意义 也就是说 不起作用！
        pros.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");


        // 创建一个kafka 消费者
        KafkaConsumer<String, String> myKafkaConsumer = new KafkaConsumer<String, String>(pros);
        //创建一个放置topic 的集合
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");

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


            //手动提交 并且为同步提交！
            myKafkaConsumer.commitSync();//会阻塞当前线程 在实时大数据开发里面我们不使用这种方式！

        }




    }
}
