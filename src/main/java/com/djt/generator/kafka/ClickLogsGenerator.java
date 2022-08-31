package com.djt.generator.kafka;

import com.alibaba.fastjson.JSON;
import com.djt.entity.ClickLogs;
import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 生成clicklog并发送到kafka指定的Topic
 */
public class ClickLogsGenerator {
    public static final String topic = "clicklog_input";

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "node02:6667");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Faker fakerZH = new Faker(new Locale("zh-CN"));
        ClickLogs clickLogs;
        Random random = new Random();
        while (true) {
            clickLogs = ClickLogs.builder()
                    .user(fakerZH.name().fullName())
                    .url(fakerZH.internet().url())
                    .cTime(sdf.format(new Date()))
                    .build();
            // 随机产生实时数据并通过Kafka生产者发送到Kafka集群
            System.out.println(JSON.toJSONString(clickLogs));
            producer.send(new ProducerRecord<String, String>(topic,JSON.toJSONString(clickLogs)));
            //随机sleep
            Thread.sleep(random.nextInt(5)*1000);
        }

    }
}
