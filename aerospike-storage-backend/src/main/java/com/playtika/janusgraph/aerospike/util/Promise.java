package com.playtika.janusgraph.aerospike.util;

import org.janusgraph.diskstorage.PermanentBackendException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public class Promise<V> {

    private final Supplier<V> supplier;
    private final Executor executor;

    private Promise(Supplier<V> supplier, Executor executor) {
        this.supplier = supplier;
        this.executor = executor;
    }

    public V get() throws ExecutionException, InterruptedException {
        return CompletableFuture.supplyAsync(supplier, executor).get();
    }

    public static <V> Promise<V> promise(Supplier<V> supplier, Executor executor){
        return new Promise<>(supplier, executor);
    }

    public static void allOf(Collection<Promise<?>> promises) throws PermanentBackendException {
        List<CompletableFuture<?>> futures = new ArrayList<>(promises.size());
        for(Promise<?> promise : promises){
            futures.add(CompletableFuture.supplyAsync(promise.supplier, promise.executor));
        }

        AsyncUtil.completeAll(futures);
    }
}
