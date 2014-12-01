package io.sberlabs.vian;

/**
 * Vian
 * Created by wal on 11/28/14.
 */

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

import kafka.javaapi.producer.Producer;

public class StreamClientInitializer extends ChannelInitializer<SocketChannel> {

    private final Producer<String, String> kafkaProducer;
    private final String kafkaTopic;
    private final String kafkaTopicPartitionBy;

    public StreamClientInitializer(Producer<String, String> kafkaProducer,
                               String kafkaTopic,
                               String kafkaTopicPartitionBy) {

        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
        this.kafkaTopicPartitionBy = kafkaTopicPartitionBy;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
        pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8));
        pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8));

        pipeline.addLast(new StreamClientHandler(kafkaProducer, kafkaTopic, kafkaTopicPartitionBy));
    }
}
