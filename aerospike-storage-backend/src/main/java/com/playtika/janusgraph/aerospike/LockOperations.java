package com.playtika.janusgraph.aerospike;

import com.aerospike.client.Value;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

interface LockOperations {

    void acquireLocks(Value transactionId, Map<Value, Map<Value, Value>> locks) throws BackendException;

    CompletableFuture<Void> releaseLockOnKeys(Collection<Value> keys) throws PermanentBackendException;
}
