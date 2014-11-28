package io.sberlabs.vian;

/**
 * Vian
 * Created by wal on 11/28/14.
 */

import com.eclipsesource.json.JsonObject;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.zip.CRC32;

public class StreamClientHandler  extends SimpleChannelInboundHandler<String> {

    static final String PARTITIONS = System.getProperty("kafka.topic.partitions", "3");

    private final String schemaId;

    public StreamClientHandler(String schemaId) {
        this.schemaId = schemaId;
    }

    private int getPartition(String s, int buckets) {
        CRC32 crc32 = new CRC32();
        crc32.update(s.getBytes(Charsets.UTF_8));
        return Hashing.consistentHash(crc32.getValue(), buckets);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        try {
            JsonObject jsonObject = JsonObject.readFrom(msg);
            String id = jsonObject.get("id").asString();
            if (id != null) {
                int partition = getPartition(id, Integer.parseInt(PARTITIONS));
                System.err.println("schemaId => "+ schemaId + "partition => " + partition + ", id => " + id);
            } else {
                System.err.println("Error: id is absent in incoming JSON message, message: " + msg);
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
