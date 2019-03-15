package com.playtika.janusgraph.aerospike;

import com.aerospike.client.*;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.janusgraph.diskstorage.locking.TemporaryLockingException;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.playtika.janusgraph.aerospike.LockOperationsUdf.LockResult.*;
import static com.playtika.janusgraph.aerospike.util.AsyncUtil.completeAll;
import static java.util.concurrent.CompletableFuture.runAsync;

class LockOperationsUdf implements LockOperations{

    public static final String PACKAGE = "check_and_lock";
    public static final String CHECK_AND_LOCK_FUNCTION_NAME = "check_and_lock";
    public static final Operation UNLOCK_OPERATION = Operation.put(new Bin("transaction", (Long) null));

    private final AerospikeClient client;
    private final AerospikeKeyColumnValueStore store;
    private final Executor aerospikeExecutor;

    LockOperationsUdf(AerospikeClient client,
                      AerospikeKeyColumnValueStore store, Executor aerospikeExecutor) {
        this.client = client;
        this.store = store;

        this.aerospikeExecutor = aerospikeExecutor;
    }

    @Override
    public void acquireLocks(Value transactionId, Map<Value, Map<Value, Value>> locks) throws BackendException {
        Map<LockResult, List<Key>> lockResults = new ConcurrentHashMap<>();

        try {

            List<CompletableFuture<?>> futures = new ArrayList<>();
            AtomicBoolean lockFailed = new AtomicBoolean(false);

            for (Map.Entry<Value, Map<Value, Value>> locksForKey : locks.entrySet()) {
                futures.add(runAsync(() -> {
                    if(lockFailed.get()){
                        return;
                    }

                    Key key = store.getKey(locksForKey.getKey());
                    LockResult lockResult = checkAndLock(client, key, transactionId, locksForKey.getValue());
                    lockResults.compute(lockResult, (result, values) -> {
                        List<Key> resultValues = values != null ? values : new ArrayList<>();
                        resultValues.add(key);
                        return resultValues;
                    });
                    if(lockResult != LOCKED){
                        lockFailed.set(true);
                    }
                }, aerospikeExecutor));
            }

            completeAll(futures);

            if(lockResults.keySet().contains(CHECK_FAILED)){
                throw new PermanentLockingException("Some pre-lock checks failed:"+lockResults.keySet());
            } else if(lockResults.keySet().contains(ALREADY_LOCKED)){
                throw new TemporaryLockingException("Some locks not released yet:"+lockResults.keySet());
            }

        } catch (Throwable t){
            releaseLocks(lockResults.get(LOCKED));
            throw t;
        }
    }

    static LockResult checkAndLock(AerospikeClient client, Key key, Value transactionId, Map<Value, Value> expectedValues) {
        return LockResult.values()[((Long) client.execute(null, key, PACKAGE, CHECK_AND_LOCK_FUNCTION_NAME,
                transactionId, Value.get(expectedValues))).intValue()];
    }

    enum LockResult {
        LOCKED,
        ALREADY_LOCKED, //previous lock not released yet
        CHECK_FAILED
    }

    @Override
    public CompletableFuture<Void> releaseLockOnKeys(Collection<Value> keys) throws PermanentBackendException {
        return releaseLocks(keys.stream()
                .map(store::getKey)
                .collect(Collectors.toList())
        );
    }

    private CompletableFuture<Void> releaseLocks(List<Key> keys) throws PermanentBackendException {
        if(keys != null && !keys.isEmpty()) {
            List<CompletableFuture<?>> futures = new ArrayList<>();
            keys.forEach(key -> futures.add(runAsync(
                    () -> client.operate(null, key, UNLOCK_OPERATION))));
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Merges locks for same columns
     * @param locksForKey
     * @return
     */
    static List<AerospikeLock> mergeLocks(List<AerospikeLock> locksForKey){
        if(locksForKey.size() <= 1){
            return locksForKey;
        }

        Map<StaticBuffer, AerospikeLock> columnToLockMap = new HashMap<>(locksForKey.size());
        for(AerospikeLock lock : locksForKey){
            columnToLockMap.putIfAbsent(lock.column, lock);
        }
        if(columnToLockMap.size() == locksForKey.size()){
            return locksForKey;
        } else {
            return new ArrayList<>(columnToLockMap.values());
        }
    }


}
