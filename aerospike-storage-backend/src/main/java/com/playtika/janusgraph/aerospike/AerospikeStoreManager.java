package com.playtika.janusgraph.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.playtika.janusgraph.aerospike.util.Promise;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.common.AbstractStoreManager;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

import java.time.Clock;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.playtika.janusgraph.aerospike.AerospikeKeyColumnValueStore.getValue;
import static com.playtika.janusgraph.aerospike.ConfigOptions.*;
import static com.playtika.janusgraph.aerospike.util.AsyncUtil.completeAll;
import static com.playtika.janusgraph.aerospike.util.Promise.allOf;
import static com.playtika.janusgraph.aerospike.util.Promise.promise;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;


@PreInitializeConfigOptions
public class AerospikeStoreManager extends AbstractStoreManager implements KeyColumnValueStoreManager {

    private static final int DEFAULT_PORT = 3000;
    static final int AEROSPIKE_BUFFER_SIZE = Integer.MAX_VALUE / 2;

    private final StoreFeatures features;

    private final AerospikeClient client;

    private final Configuration configuration;

    private final WriteAheadLogManager writeAheadLogManager;

    private final ThreadPoolExecutor scanExecutor;

    private final ThreadPoolExecutor aerospikeExecutor;

    public AerospikeStoreManager(Configuration configuration) {
        super(configuration);

        Preconditions.checkArgument(configuration.get(BUFFER_SIZE) == AEROSPIKE_BUFFER_SIZE,
                "Set unlimited buffer size as we use deferred locking approach");

        int port = storageConfig.has(STORAGE_PORT) ? storageConfig.get(STORAGE_PORT) : DEFAULT_PORT;

        Host[] hosts = Stream.of(configuration.get(STORAGE_HOSTS))
                .map(hostname -> new Host(hostname, port)).toArray(Host[]::new);

        ClientPolicy clientPolicy = new ClientPolicy();
//        clientPolicy.user = storageConfig.get(AUTH_USERNAME);
//        clientPolicy.password = storageConfig.get(AUTH_PASSWORD);
        clientPolicy.writePolicyDefault.sendKey = true;
        clientPolicy.readPolicyDefault.sendKey = true;
        clientPolicy.scanPolicyDefault.sendKey = true;

        client = new AerospikeClient(clientPolicy, hosts);

        this.configuration = configuration;

        features = new StandardStoreFeatures.Builder()
                .keyConsistent(configuration)
                .persists(true)
                //here we promise to take care of locking.
                //If false janusgraph will do it via ExpectedValueCheckingStoreManager that is less effective
                .locking(true)
                //caused by deferred locking approach used in this storage backend,
                //actual locking happens just before transaction commit
                .optimisticLocking(true)
                .transactional(false)
                .distributed(true)
                .multiQuery(true)
                .batchMutation(true)
                .unorderedScan(true)
                .orderedScan(false)
                .keyOrdered(false)
                .localKeyPartition(false)
                .timestamps(false)
                .supportsInterruption(false)
                .build();

        registerUdfs(client);

        writeAheadLogManager = new WriteAheadLogManager(client, configuration.get(WAL_NAMESPACE), Clock.systemUTC());

        scanExecutor = new ThreadPoolExecutor(0, configuration.get(SCAN_PARALLELISM),
                1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());

        aerospikeExecutor = new ThreadPoolExecutor(4, configuration.get(AEROSPIKE_PARALLELISM),
                1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());
    }

    static void registerUdfs(AerospikeClient client){
        client.register(null, AerospikeStoreManager.class.getClassLoader(),
                "udf/check_and_lock.lua", "check_and_lock.lua", Language.LUA);
    }

    @Override
    public AerospikeKeyColumnValueStore openDatabase(String name) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Database name may not be null or empty");

        return new AerospikeKeyColumnValueStore(name, client, configuration, aerospikeExecutor, scanExecutor, writeAheadLogManager);
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) {
        return openDatabase(name);
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) {
        return new AerospikeTransaction(config);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        Map<String, Map<Value, Map<Value, Value>>> locksByStore = groupLocksByStoreKeyColumn(
                ((AerospikeTransaction) txh).getLocks());

        Map<String, Map<Value, Map<Value, Value>>> mutationsByStore = groupMutationsByStoreKeyColumn(mutations);

        Value transactionId = writeAheadLogManager.writeTransaction(locksByStore, mutationsByStore);

        acquireLocks(transactionId, locksByStore);
        try {
            Map<String, Set<Value>> mutatedByStore = mutateMany(mutationsByStore, locksByStore);
            releaseLocks(locksByStore, mutatedByStore);
        } catch (AerospikeException e) {
            releaseLocks(locksByStore, emptyMap());
            throw new PermanentBackendException(e);
        } finally {
            writeAheadLogManager.deleteTransaction(transactionId);
        }
    }

    static Map<String, Map<Value, Map<Value, Value>>> groupLocksByStoreKeyColumn(List<AerospikeLock> locks){
        return locks.stream()
                .collect(Collectors.groupingBy(lock -> lock.storeName,
                        Collectors.groupingBy(lock -> getValue(lock.key),
                                Collectors.toMap(
                                        lock -> getValue(lock.column),
                                        lock -> lock.expectedValue != null ? getValue(lock.expectedValue) : Value.NULL,
                                        (oldValue, newValue) -> oldValue))));
    }

    private static Map<String, Map<Value, Map<Value, Value>>> groupMutationsByStoreKeyColumn(
            Map<String, Map<StaticBuffer, KCVMutation>> mutationsByStore){
        Map<String, Map<Value, Map<Value, Value>>> mapByStore = new HashMap<>(mutationsByStore.size());
        for(Map.Entry<String, Map<StaticBuffer, KCVMutation>> storeMutations : mutationsByStore.entrySet()) {
            Map<Value, Map<Value, Value>> map = new HashMap<>(storeMutations.getValue().size());
            for (Map.Entry<StaticBuffer, KCVMutation> mutationEntry : storeMutations.getValue().entrySet()) {
                map.put(getValue(mutationEntry.getKey()), mutationToMap(mutationEntry.getValue()));
            }
            mapByStore.put(storeMutations.getKey(), map);
        }
        return mapByStore;
    }

    static Map<Value, Value> mutationToMap(KCVMutation mutation){
        Map<Value, Value> map = new HashMap<>(mutation.getAdditions().size() + mutation.getDeletions().size());
        for(StaticBuffer deletion : mutation.getDeletions()){
            map.put(getValue(deletion), Value.NULL);
        }

        for(Entry addition : mutation.getAdditions()){
            map.put(getValue(addition.getColumn()), getValue(addition.getValue()));
        }
        return map;
    }

    private Map<String, Set<Value>> mutateMany(
            Map<String, Map<Value, Map<Value, Value>>> mutationsByStore,
            Map<String, Map<Value, Map<Value, Value>>> locksByStore) throws PermanentBackendException {

        //first mutate not locked keys so need to split mutations
        List<Promise<?>> notLockedMutations = new ArrayList<>();
        List<Promise<?>> lockedMutations = new ArrayList<>();
        Map<String, Set<Value>> mutatedByStore = new ConcurrentHashMap<>();

        mutationsByStore.forEach((storeName, storeMutations) -> {
            final AerospikeKeyColumnValueStore store = openDatabase(storeName);
            Map<Value, Map<Value, Value>> storeLocks = locksByStore.get(storeName);
            for(Map.Entry<Value, Map<Value, Value>> mutationEntry : storeMutations.entrySet()){
                Value key = mutationEntry.getKey();
                Map<Value, Value> mutation = mutationEntry.getValue();
                Promise<?> mutationPromise = promise(() -> {
                    store.mutate(key, mutation);
                    mutatedByStore.compute(storeName, (s, keys) -> {
                        Set<Value> keysResult = keys != null ? keys : new HashSet<>();
                        keysResult.add(key);
                        return keysResult;
                    });
                    return null;
                }, aerospikeExecutor);
                if(storeLocks != null && storeLocks.containsKey(key)){
                    lockedMutations.add(mutationPromise);
                } else {
                    notLockedMutations.add(mutationPromise);
                }
            }
        });

        allOf(notLockedMutations);
        allOf(lockedMutations);

        return mutatedByStore;
    }

    private void acquireLocks(
            Value transactionId,
            Map<String, Map<Value, Map<Value, Value>>> locks) throws BackendException {
        for(Map.Entry<String, Map<Value, Map<Value, Value>>> entry : locks.entrySet()){
            String storeName = entry.getKey();
            final AerospikeKeyColumnValueStore store = openDatabase(storeName);
            store.getLockOperations().acquireLocks(transactionId, entry.getValue());
        }
    }

    private void releaseLocks(Map<String, Map<Value, Map<Value, Value>>> locksByStore,
                              Map<String, Set<Value>> mutatedByStore) throws PermanentBackendException {
        List<CompletableFuture<?>> storeFutures = new ArrayList<>(locksByStore.size());
        for(Map.Entry<String, Map<Value, Map<Value, Value>>> storeLocks : locksByStore.entrySet()){
            String storeName = storeLocks.getKey();
            final AerospikeKeyColumnValueStore store = openDatabase(storeName);
            Set<Value> mutatedForStore = mutatedByStore.get(storeName);
            List<Value> keysToRelease = storeLocks.getValue().keySet().stream()
                    //ignore mutated keys as they already have been released
                    .filter(key -> !mutatedForStore.contains(key))
                    .collect(Collectors.toList());
            storeFutures.add(store.getLockOperations().releaseLockOnKeys(keysToRelease));
        }

        completeAll(storeFutures);
    }

    @Override
    public void close() throws BackendException {
        try {
            scanExecutor.shutdown();
            aerospikeExecutor.shutdown();
            client.close();
        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            while(!emptyStorage()){
                client.truncate(null, configuration.get(NAMESPACE), null, null);
                Thread.sleep(100);
            }

        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean exists() throws BackendException {
        try {
            return !emptyStorage();
        } catch (AerospikeException e) {
            throw new PermanentBackendException(e);
        }
    }

    private boolean emptyStorage(){
        String namespace = configuration.get(NAMESPACE);
        String answer = Info.request(client.getNodes()[0], "sets/" + namespace);
        return Stream.of(answer.split(";"))
                .allMatch(s -> s.contains("objects=0"));
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + ":" + "HARDCODED";
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() {
        throw new UnsupportedOperationException();
    }




}
