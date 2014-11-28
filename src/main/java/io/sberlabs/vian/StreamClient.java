package io.sberlabs.vian;

/**
 * Vian
 * Created by wal on 11/28/14.
 */

import com.github.kevinsawicki.http.HttpRequest;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.nio.file.Files;
import java.nio.file.Paths;

public class StreamClient {

    static final String REPO_URL = System.getProperty("avro.schema.repo.url", "http://hotel:2876/schema-repo");
    static final String REPO_GROUP = System.getProperty("avro.schema.repo.group", "io.sberlabs.records");
    static final String PROXY_HOST = System.getProperty("zmq.proxy.host", "bravo");
    static final String PROXY_PORT = System.getProperty("zmq.proxy.port", "5353");
    static final String SCHEMA = System.getProperty("schema.file", "/home/wal/work/java/vian/src/main/avro/rutarget.avsc");

    public static void main(String[] args) throws Exception {

        String schema = new String(Files.readAllBytes(Paths.get(SCHEMA)));

        HttpRequest request = HttpRequest
                .put(REPO_URL + "/" + REPO_GROUP + "/register")
                .contentType("text/plain")
                .send(schema);
        String schemaId = request.body();
        int responseStatus = request.code();

        if (responseStatus != 200) {
            System.err.println("Error in registering schema, status = " + responseStatus);
            System.exit(1);
        } else {
            System.err.println("Schema registered successfully, schemaId =  " + schemaId + ", status = " + responseStatus);
        }

        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();

            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new StreamClientInitializer(schemaId));

            ChannelFuture f = b.connect(PROXY_HOST, Integer.parseInt(PROXY_PORT)).sync();

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
        }
    }
}
