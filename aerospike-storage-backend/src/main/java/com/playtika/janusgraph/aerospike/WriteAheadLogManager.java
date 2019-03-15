package com.playtika.janusgraph.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class WriteAheadLogManager {

    private static final String WAL_SET_NAME = "wal";

    private static final String UUID_BIN = "uuid";
    private static final String TIMESTAMP_BIN = "timestamp";
    private static final String LOCKS_BIN = "locks";
    private static final String MUTATIONS_BIN = "mutations";

    private final AerospikeClient aerospikeClient;
    private final String walNamespace;
    private final Clock clock;

    public WriteAheadLogManager(AerospikeClient aerospikeClient, String walNamespace, Clock clock) {
        this.aerospikeClient = aerospikeClient;
        this.walNamespace = walNamespace;
        this.clock = clock;
    }

    public Value writeTransaction(Map<String, Map<Value, Map<Value, Value>>> locks,
                                  Map<String, Map<Value, Map<Value, Value>>> mutations){

        Value transactionId = Value.get(getBytesFromUUID(UUID.randomUUID()));
        aerospikeClient.put(null,
                new Key(walNamespace, WAL_SET_NAME, transactionId),
                new Bin(UUID_BIN, transactionId),
                new Bin(TIMESTAMP_BIN, Value.get(clock.millis())),
                new Bin(LOCKS_BIN, stringMapToValue(locks)),
                new Bin(MUTATIONS_BIN, stringMapToValue(mutations)));
        return transactionId;
    }

    public void deleteTransaction(Value transactionId) {
        aerospikeClient.delete(null, new Key(walNamespace, WAL_SET_NAME, transactionId));
    }

    private Value stringMapToValue(Map<String, Map<Value, Map<Value, Value>>> map){
        Map<Value, Map<Value, Map<Value, Value>>> locksValue = new HashMap<>(map.size());
        for(Map.Entry<String, Map<Value, Map<Value, Value>>> locksEntry : map.entrySet()){
            locksValue.put(Value.get(locksEntry.getKey()), locksEntry.getValue());
        }
        return Value.get(locksValue);
    }

    public static byte[] getBytesFromUUID(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());

        return bb.array();
    }
}
