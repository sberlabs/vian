package io.sberlabs.vian;

/**
 * Vian
 * Created by wal on 11/28/14.
 */

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class StreamClient {

    static final String REPO_URL = System.getProperty("avro.schema.repo.url", "http://hotel:2876/schema-repo");
    static final String REPO_GROUP = System.getProperty("avro.schema.repo.group", "io.sberlabs.records");
    static final String SCHEMA_CLASS_NAME = System.getProperty("avro.schema.class",
            "io.sberlabs.records.Rutarget");

    static final String PROXY_HOST = System.getProperty("zmq.proxy.host", "bravo");
    static final String PROXY_PORT = System.getProperty("zmq.proxy.port", "5353");

    static final String KAFKA_BROKER_LIST = System.getProperty("kafka.broker.list",
            "hotel:9092,india:9092,juliett:9092");
    static final String KAFKA_SERIALIZER = System.getProperty("kafka.serializer.class",
            "io.sberlabs.vian.CamusSerializerForMessages");
    static final String KAFKA_KEY_SERIALIZER = System.getProperty("kafka.key.serializer.class",
            "io.sberlabs.vian.CamusSerializerForKeys");
    static final String KAFKA_PARTITIONER = System.getProperty("kafka.partitioner.class",
            "io.sberlabs.vian.ConsistentHashingPartitioner");
    static final String KAFKA_ACK_LEVEL = System.getProperty("kafka.ack.level", "0");
    static final String KAFKA_TOPIC = System.getProperty("kafka.topic", "rutarget-clickstream");
    static final String KAFKA_TOPIC_PART_BY = System.getProperty("kafka.topic.partition.by", "id");

    public static void main(String[] args) throws Exception {

        System.setProperty("avro.schema.repo.url", REPO_URL);
        System.setProperty("avro.schema.repo.group", REPO_GROUP);
        System.setProperty("avro.schema.class", SCHEMA_CLASS_NAME);

        Properties props = new Properties();
        props.put("metadata.broker.list", KAFKA_BROKER_LIST);
        props.put("serializer.class", KAFKA_SERIALIZER);
        props.put("key.serializer.class", KAFKA_KEY_SERIALIZER);
        props.put("partitioner.class", KAFKA_PARTITIONER);
        props.put("request.required.acks", KAFKA_ACK_LEVEL);

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<>(config);

        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();

            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new StreamClientInitializer(producer, KAFKA_TOPIC, KAFKA_TOPIC_PART_BY));

            ChannelFuture f = b.connect(PROXY_HOST, Integer.parseInt(PROXY_PORT)).sync();

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}
