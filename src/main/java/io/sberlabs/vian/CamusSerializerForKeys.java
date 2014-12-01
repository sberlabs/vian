package io.sberlabs.vian;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
 * Vian
 * Created by wal on 11/30/14.
 */

public class CamusSerializerForKeys implements Encoder<String> {

    public CamusSerializerForKeys(VerifiableProperties verifiableProperties) {

    }

    @Override
    public byte[] toBytes(String key) {

        return key.getBytes();
    }
}
