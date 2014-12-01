package io.sberlabs.vian;

/**
 * Vian
 * Created by wal on 11/30/14.
 */
import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import java.util.zip.CRC32;

public class ConsistentHashingPartitioner implements Partitioner {
    public ConsistentHashingPartitioner(VerifiableProperties verifiableProperties) {

    }

    public int partition(Object key, int a_numPartitions) {
        String stringKey = (String) key;
        CRC32 crc32 = new CRC32();
        crc32.update(stringKey.getBytes(Charsets.UTF_8));
        return Hashing.consistentHash(crc32.getValue(), a_numPartitions);
    }
}