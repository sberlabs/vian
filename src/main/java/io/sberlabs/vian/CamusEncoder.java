package io.sberlabs.vian;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
 * Vian
 * Created by wal on 11/30/14.
 */

public class CamusEncoder implements Encoder<String> {
    public CamusEncoder(VerifiableProperties verifiableProperties) {
        /* This constructor must be present for successful compile. */
    }

    @Override
    public byte[] toBytes(String message) {
        return message.getBytes();
    }
}