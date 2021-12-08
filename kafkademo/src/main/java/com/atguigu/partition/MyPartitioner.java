package com.atguigu.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 1. 实现接口Partitioner
 * 2. 实现3个方法:partition,close,configure
 * 3. 编写partition方法,返回分区号
 */



public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //获取消息
        String s = value.toString();
        //创建partition
        int partition;
        //判断是否包含atguigu
        if (s.contains("atguigu")){
            partition = 0;
        }else {
            partition = 1;
        }
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
