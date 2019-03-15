package com.playtika.janusgraph.aerospike;

import org.janusgraph.diskstorage.configuration.ConfigOption;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NS;

public class ConfigOptions {

    public static final ConfigOption<String> NAMESPACE = new ConfigOption<>(STORAGE_NS,
            "namespace", "Aerospike namespace to use", ConfigOption.Type.LOCAL, "test");

    public static final ConfigOption<String> WAL_NAMESPACE = new ConfigOption<>(STORAGE_NS,
            "wal-namespace", "Aerospike namespace to use for write ahead log", ConfigOption.Type.LOCAL, "test");

    public static final ConfigOption<Boolean> ALLOW_SCAN = new ConfigOption<>(STORAGE_NS,
            "allow-scan", "Whether to allow scans on graph. Can't be changed after graph creation", ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<Integer> SCAN_PARALLELISM = new ConfigOption<>(STORAGE_NS,
            "scan-parallelism", "How many threads may perform scan operations simultaneously", ConfigOption.Type.LOCAL, 1);

    public static final ConfigOption<Integer> AEROSPIKE_PARALLELISM = new ConfigOption<>(STORAGE_NS,
            "aerospike-parallelism", "Limits how many parallel calls allowed to aerospike", ConfigOption.Type.LOCAL, 100);

}
