package com.atguigu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

//  不带回调函数的API

/*
    KafkaProducer：需要创建一个生产者对象，用来发送数据
    ProducerConfig：获取所需的一系列配置参数
    ProducerRecord：每条数据都要封装成一个ProducerRecord对象
 */


public class MyKafkaProducer {
    public static void main(String[] args) {
        // 1. 创建kafka生产者的配置对象
        Properties props = new Properties();

        //设置配置文件内容 ： 1.需要配置Kafka集群地址:该地址可以找到我们的Kafka集群 一般工作中写两个地址：
            //hadoop102:9092,hadoop103:9092, 以防其中一个broker挂掉
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092");

        // key,value序列化
            //一般情况下，都是String,因为String这个数据类型java提供的api相对较多
            //并且我们需要注意的是，一定要使用官方提供的序列化器；
        // props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //批次大小 单位是byte 默认16k 只有数据积累到batch.size之后，sender才会发送数据。
        props.put("batch.size", 16384);

        //等待时间 如果数据迟迟未达到batch.size，sender等待linger.time之后就会发送数据。
        //设置为0不等待 一般工作中使用Kafka的时候设置为0,因为我们的Kafka的使用场景一般为 实时处理大数据场景
        props.put("linger.ms",1);

        //2.new 一个Kafka的生产者
        KafkaProducer<String, String> myKafkaProducer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++) {
            //发送数据
            ProducerRecord<String, String> myProducerRecord = new ProducerRecord<String, String>("first","Kafka"+i);
            myKafkaProducer.send(myProducerRecord);
        }

        //3.关闭资源
        myKafkaProducer.close(); //生命周期函数，它的底层都带有一个flush()函数，这样就会把我们的数据刷写出去

    }
}
