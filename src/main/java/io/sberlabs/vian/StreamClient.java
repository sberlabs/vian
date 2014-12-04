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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class StreamClient {

    private static void printHelp() {

        System.out.println("Usage: vian <path-to-vian.properties>");
    }

    private static void configure(String filename) {

        File config = new File(filename);

        if (!config.canRead()) {
            System.err.println("Cannot read file: " + config);
            printHelp();
            System.exit(1);
        }

        Properties props = new Properties();
        try {
            props.load(new BufferedInputStream(new FileInputStream(config)));
            Properties systemProps = System.getProperties();
            systemProps.putAll(props);
            System.setProperties(systemProps);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            printHelp();
            System.exit(1);
        }

        configure(args[0]);

        Properties props = new Properties();
        props.put("metadata.broker.list", System.getProperty("kafka.broker.list"));
        props.put("serializer.class", System.getProperty("kafka.serializer.class"));
        props.put("key.serializer.class", System.getProperty("kafka.key.serializer.class"));
        props.put("partitioner.class", System.getProperty("kafka.partitioner.class"));
        props.put("request.required.acks", System.getProperty("kafka.ack.level"));
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<>(config);

        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();

            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new StreamClientInitializer(producer,
                    System.getProperty("kafka.topic"),
                    System.getProperty("kafka.topic.partition.by")));

            ChannelFuture f = b.connect(System.getProperty("zmq.proxy.host"),
                    Integer.parseInt(System.getProperty("zmq.proxy.port"))).sync();

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}
