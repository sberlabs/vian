package io.sberlabs.vian;

import io.sberlabs.records.Rutarget;

import com.eclipsesource.json.JsonObject;
import com.github.kevinsawicki.http.HttpRequest;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import org.apache.avro.Schema;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Vian
 * Created by wal on 11/30/14.
 */

public class CamusSerializerForMessages implements Encoder<String> {

    private static final byte MAGIC_BYTE = 0x0;
    private String schemaId;

    public CamusSerializerForMessages(VerifiableProperties verifiableProperties) {
        Schema schema;
        SpecificRecord obj;

        String repoUrl = System.getProperty("avro.schema.repo.url");
        String repoGroup = System.getProperty("avro.schema.repo.group");
        String schemaClassName = System.getProperty("avro.schema.class");

        try {
            Class<?> clazz = Class.forName(schemaClassName);
            obj = (SpecificRecord) clazz.newInstance();
            schema = obj.getSchema();

            HttpRequest request = HttpRequest
                    .put(repoUrl + "/" + repoGroup + "/register")
                    .contentType("text/plain")
                    .send(schema.toString());
            schemaId = request.body();
            int responseStatus = request.code();

            if (responseStatus != 200) {
                System.err.println("Error in registering schema, status = " + responseStatus);
                System.exit(1);
            } else {
                System.err.println("Schema registered successfully, schemaId =  " + schemaId + ", status = " + responseStatus);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private Rutarget jsonToDatum(String s) {
        JsonObject j = JsonObject.readFrom(s);
        boolean ipTruncated = false;
        String ipAddress = "0.0.0.0";

        if (j.get("ip_trunc") != null) {
            ipTruncated = j.get("ip_trunc").asBoolean();
        }

        if (j.get("ip") != null) {
            ipAddress = j.get("ip").asString();
        }

        return Rutarget.newBuilder()
                .setId(j.get("id").asString())
                .setTs(j.get("ts").asLong())
                .setIp(ipAddress)
                .setIpTrunc(ipTruncated)
                .setUrl(j.get("url").asString())
                .setUa(j.get("ua").asString())
                .build();
    }

    @Override
    public byte[] toBytes(String json) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        SpecificDatumWriter<Rutarget> writer;
        org.apache.avro.io.Encoder encoder;
        try {
            output.write(MAGIC_BYTE);
            output.write(ByteBuffer.allocate(4).putInt(Integer.parseInt(schemaId)).array());

            writer = new SpecificDatumWriter<>(Rutarget.class);
            encoder = EncoderFactory.get().binaryEncoder(output, null);

            Rutarget datum = jsonToDatum(json);
            writer.write(datum, encoder);
            encoder.flush();

            return output.toByteArray();
        } catch (IOException e) {

        }
        return null;
    }
}
