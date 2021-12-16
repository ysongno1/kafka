package com.atguigu.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/*
    同步发送的意思就是，一条消息发送之后，会阻塞当前线程，直至返回ack。
    由于send方法返回的是一个Future对象，根据Futrue对象的特点，我们也可以实现同步发送的效果，
    只需在调用Future对象的get方发即可。

 */
//  带回调函数的API

/*
    KafkaProducer：需要创建一个生产者对象，用来发送数据
    ProducerConfig：获取所需的一系列配置参数
    ProducerRecord：每条数据都要封装成一个ProducerRecord对象
 */


public class MyProducerCallbackGet {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // 1. 创建kafka生产者的配置对象
        Properties props = new Properties();

        //设置配置文件内容 ： 1.需要配置Kafka集群地址:该地址可以找到我们的Kafka集群 一般工作中写两个地址：
        //hadoop102:9092,hadoop103:9092, 以防其中一个broker挂掉
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092");

        // key,value序列化
        // props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //批次大小 默认16k 只有数据积累到batch.size之后，sender才会发送数据。
        props.put("batch.size", 16384);

        //等待时间 如果数据迟迟未达到batch.size，sender等待linger.time之后就会发送数据。
        props.put("linger.ms", 0);

        // 设置ack
        //acks参数指定了在集群中有多少个分区副本收到消息，kafka producer才会认为消息是被写入成功。
        props.put("acks", "all");

        //2.new 一个Kafka的生产者
        KafkaProducer<String, String> myKafkaProducer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++) {
            //发送数据
            //先在kafka shell命令行中创建了一个second 的topic 分区数为5
            //kafka-topics.sh --bootstrap-server hadoop102:9092 --create --topic second --partitions 5
            ProducerRecord<String, String> myProducerRecord = new ProducerRecord<String, String>("second", "Kafka" + i);
            myKafkaProducer.send(myProducerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        // 没有异常,输出信息到控制台
                        System.out.println("offset："+ metadata.offset() +" partition："+ metadata.partition()+" topic："+metadata.topic());
                    } else {
                        // 出现异常打印
                        exception.printStackTrace();
                    }
                }
            }).get();
            //	同步发送的意思就是，一条消息发送之后，会阻塞当前线程，直至返回ack。
            //由于send方法返回的是一个Future对象，根据Futrue对象的特点，我们也可以实现同步发送的效果，
            // 只需在调用Future对象的get方发即可。

            Thread.sleep(100); //使用线程睡眠时间 是我们发送数据的时候 不按照批次发送的条件来发送消息
        }

        //3.关闭资源
        myKafkaProducer.close(); //生命周期函数，它的底层都带有一个flush()函数，这样就会把我们的数据刷写出去

    }
}
