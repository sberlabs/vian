package io.sberlabs.vian;

/**
 * Vian
 * Created by wal on 11/28/14.
 */

import com.eclipsesource.json.JsonObject;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

public class StreamClientHandler extends SimpleChannelInboundHandler<String> {

    private final Producer<String, String> kafkaProducer;
    private final String kafkaTopic;
    private final String kafkaTopicPartitionBy;

    public StreamClientHandler(Producer<String, String> kafkaProducer,
                               String kafkaTopic,
                               String kafkaTopicPartitionBy) {

        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
        this.kafkaTopicPartitionBy = kafkaTopicPartitionBy;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        try {
            JsonObject jsonObject = JsonObject.readFrom(msg);
            String partitionKey = jsonObject.get(kafkaTopicPartitionBy).asString();

            if (partitionKey != null) {
                KeyedMessage<String, String> data = new KeyedMessage<>(kafkaTopic, partitionKey, msg);
                kafkaProducer.send(data);
            } else {
                System.err.println("Error: partition key is absent in incoming JSON message, message: " + msg);
            }
        } catch (RuntimeException e) {
            System.err.println("Error: " + e.getMessage() + ", message: " + msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
