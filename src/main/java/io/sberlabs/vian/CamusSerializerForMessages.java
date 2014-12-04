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

    private static HttpRequest avroRepoRegisterSchema(String schemaString) {
        String repoUrl = System.getProperty("avro.schema.repo.url");
        String repoSubj = System.getProperty("avro.schema.repo.subject");

        return HttpRequest
                .put(repoUrl + "/" + repoSubj + "/register")
                .contentType("text/plain")
                .send(schemaString);
    }

    private static HttpRequest avroRepoRegisterSubject() {
        String repoUrl = System.getProperty("avro.schema.repo.url");
        String repoSubj = System.getProperty("avro.schema.repo.subject");

        return HttpRequest
                .put(repoUrl + "/" + repoSubj)
                .contentType("application/x-www-form-urlencoded");
    }

    private static String registerSchema(String schemaString) {
        HttpRequest req = avroRepoRegisterSchema(schemaString);

        int code = req.code();
        String body = req.body();

        if (code == 200) {
            System.err.println("Schema registered successfully, schemaId =  " + body);
            return body;
        }
        else if (code == 404) {
            System.err.println("Subject is not in schema repo, trying to register it...");
            int codeSubj = avroRepoRegisterSubject().code();

            if (codeSubj != 200) {
                System.err.println("Error in registering subject, status = " + codeSubj);
                return "";
            }

            System.err.println("Subject has been registered successfully.");
            return registerSchema(schemaString);
        } else {
            System.err.println("Error in registering schema, status = " + code);
            return "";
        }
    }

    public CamusSerializerForMessages(VerifiableProperties verifiableProperties) {
        Schema schema;
        SpecificRecord obj;

        String schemaClassName = System.getProperty("avro.schema.class");

        try {
            Class<?> clazz = Class.forName(schemaClassName);
            obj = (SpecificRecord) clazz.newInstance();
            schema = obj.getSchema();
            schemaId = registerSchema(schema.toString());
            if (schemaId.equals("")) {
                System.exit(1);
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
        String ua = "ua-is-absent";
        String url = "url-is-absent";

        if (j.get("ip_trunc") != null) {
            ipTruncated = j.get("ip_trunc").asBoolean();
        }

        if (j.get("ip") != null) {
            ipAddress = j.get("ip").asString();
        }

        if (j.get("ua") != null) {
            ua = j.get("ua").asString();
        }

        if (j.get("url") != null) {
            url = j.get("url").asString();
        }

        return Rutarget.newBuilder()
                .setId(j.get("id").asString())
                .setTs(j.get("ts").asLong())
                .setIp(ipAddress)
                .setIpTrunc(ipTruncated)
                .setUrl(url)
                .setUa(ua)
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
