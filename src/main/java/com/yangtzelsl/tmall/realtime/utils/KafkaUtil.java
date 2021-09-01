package com.yangtzelsl.tmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KafkaUtil {

    private static final String KAFKA_SERVER = "hd1:9092,hd2:9092,hd3:9092";

    /**
     * 获取 kafka 通用配置
     * @return 配置类
     */
    private static Properties getProps() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        return properties;
    }

    /**
     * 通过 topic 和 groupId 创建一个 Kafka Source
     * @param topic
     * @param groupId
     * @return Kafka Source
     */
    public static SourceFunction<String> ofSource(String topic, String groupId) {
        Properties props = getProps();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
    }

    /**
     * 通过 topic 创建一个字符串序列化的 Kafka Sink
     * @param topic
     * @return Kafka Sink
     */
    public static SinkFunction<String> ofSink(String topic) {
        return new FlinkKafkaProducer<>(topic, new SimpleStringSchema(), getProps());
    }

    /**
     * 通过 序列化器 创建一个 Kafka Sink
     * @param serializationSchema
     * @param <IN>
     * @return Kafka Sink
     */
    public static <IN> SinkFunction<IN> ofSink(KafkaSerializationSchema<IN> serializationSchema) {
        return new FlinkKafkaProducer<>("default_topic", serializationSchema, getProps(), FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    /**
     * 通过 topic 和 groupId 生成一个 Flink SQL 的 Kafka 连接信息
     * @param topic
     * @param groupId
     * @return
     */
    public static String getKafkaDDL(String topic, String groupId) {
        return " 'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset' ";
    }
}

