/**
 * Copyright 2016 Neeve Research, LLC
 *
 * This product includes software developed at Neeve Research, LLC
 * (http://www.neeveresearch.com/) as well as software licenced to
 * Neeve Research, LLC under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * Neeve Research licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.neeve.perf.aep.engine;

final class ConfigProperties {
    // Driver
    final static String PROP_DRIVER_TEST_ENCODING = "DRIVER_TEST_ENCODING";
    final static String PROP_DRIVER_TEST_COUNT = "DRIVER_TEST_COUNT";
    final static String PROP_DRIVER_TEST_WARMUP_TIME = "DRIVER_TEST_WARMUP_TIME";
    final static String PROP_DRIVER_TEST_RATE = "DRIVER_TEST_RATE";
    final static String PROP_DRIVER_TEST_EMPTY_MESSAGE = "DRIVER_TEST_EMPTY_MESSAGE";
    final static String PROP_DRIVER_INJECTOR_CPU_AFFINITY_MASK = "DRIVER_INJECTOR_CPU_AFFINITY_MASK";
    final static String PROP_DRIVER_LW_NOWRITE = "DRIVER_LW_NOWRITE";
    final static String PROP_DRIVER_LW_PRINT_INTERVAL_STATS = "DRIVER_LW_PRINT_INTERVAL_STATS";
    final static String PROP_DRIVER_PROMPT_TO_START = "DRIVER_PROMPT_TO_START";

    // Output
    final static String PROP_OUTPUT_FILE = "OUTPUT_FILE";
    final static String PROP_OUTPUT_CELL = "OUTPUT_CELL";
    final static String PROP_OUTPUT_THROUGHPUT = "OUTPUT_THROUGHPUT";

    // Multiplexer
    final static String PROP_MUX_QUEUE_DEPTH = "MUX_QUEUE_DEPTH";
    final static String PROP_MUX_CPU_AFFINITY_MASK = "MUX_CPU_AFFINITY_MASK";
    
    // Bus
    final static String PROP_BUS_DETACHED_SEND = "BUS_DETACHED_SEND";
    final static String PROP_BUS_DETACHED_SEND_QUEUE_DEPTH = "BUS_DETACHED_SEND_QUEUE_DEPTH";
    final static String PROP_BUS_DETACHED_SEND_QUEUE_DRAINER_CPU_AFFINITY_MASK = "BUS_DETACHED_SEND_QUEUE_DRAINER_CPU_AFFINITY_MASK";
    
    // Storage
    final static String PROP_STORAGE_ENABLED = "STORAGE_ENABLED";

    // Persistence
    final static String PROP_PERSISTENCE_ENABLED = "PERSISTENCE_ENABLED";
    final static String PROP_PERSISTENCE_LOG_LOCATION = "PERSISTENCE_LOG_LOCATION";
    final static String PROP_PERSISTENCE_INITIAL_LOG_LENGTH = "PERSISTENCE_INITIAL_LOG_LENGTH";
    final static String PROP_PERSISTENCE_ZERO_OUT_INITIAL = "PERSISTENCE_ZERO_OUT_INITIAL";
    final static String PROP_PERSISTENCE_WRITE_BUFFER_SIZE = "PERSISTENCE_WRITE_BUFFER_SIZE";
    final static String PROP_PERSISTENCE_FLUSH_USING_MAPPED_MEMORY = "PERSISTENCE_FLUSH_USING_MAPPED_MEMORY";
    final static String PROP_PERSISTENCE_FLUSH_ON_COMMIT = "PERSISTENCE_FLUSH_ON_COMMIT";
    final static String PROP_PERSISTENCE_DETACHED = "PERSISTENCE_DETACHED";
    final static String PROP_PERSISTENCE_DETACHED_QUEUE_DEPTH = "PERSISTENCE_DETACHED_QUEUE_DEPTH";
    final static String PROP_PERSISTENCE_DETACHED_QUEUE_DRAINER_CPU_AFFINITY_MASK = "PERSISTENCE_DETACHED_QUEUE_DRAINER_CPU_AFFINITY_MASK";
    final static String PROP_PERSISTENCE_READ_BUFFER_SIZE = "PERSISTENCE_READ_BUFFER_SIZE";
    final static String PROP_PERSISTENCE_PAGE_SIZE = "PERSISTENCE_PAGE_SIZE";
    final static String PROP_PERSISTENCE_QUORUM = "PERSISTENCE_QUORUM";

    // Clustering
    final static String PROP_CLUSTERING_ENABLED = "CLUSTERING_ENABLED";
    final static String PROP_CLUSTERING_LOCAL_IF_ADDR = "CLUSTER_REPLICATOR_LOCALIFADDR";
    final static String PROP_CLUSTERING_LOCAL_PORT = "CLUSTER_REPLICATOR_LOCALPORT";
    final static String PROP_CLUSTERING_DETACHED_SEND = "CLUSTERING_DETACHED_SEND";
    final static String PROP_CLUSTERING_DETACHED_SEND_QUEUE_DEPTH = "CLUSTERING_DETACHED_SEND_QUEUE_DEPTH";
    final static String PROP_CLUSTERING_DETACHED_SEND_QUEUE_DRAINER_CPU_AFFINITY_MASK = "CLUSTERING_DETACHED_SEND_QUEUE_DRAINER_CPU_AFFINITY_MASK";
    final static String PROP_CLUSTERING_DETACHED_DISPATCH = "CLUSTERING_DETACHED_DISPATCH";
    final static String PROP_CLUSTERING_DETACHED_DISPATCH_QUEUE_DEPTH = "CLUSTERING_DETACHED_DISPATCH_QUEUE_DEPTH";
    final static String PROP_CLUSTERING_DETACHED_DISPATCH_QUEUE_DRAINER_CPU_AFFINITY_MASK = "CLUSTERING_DETACHED_DISPATCH_QUEUE_DRAINER_CPU_AFFINITY_MASK";


}

