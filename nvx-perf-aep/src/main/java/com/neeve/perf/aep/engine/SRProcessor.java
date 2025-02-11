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

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import jargs.gnu.CmdLineParser;

import com.neeve.aep.AepEngine;
import com.neeve.aep.AepEngineDescriptor;
import com.neeve.aep.AepMessageSender;
import com.neeve.aep.IAepApplicationStateFactory;
import com.neeve.aep.annotations.EventHandler;
import com.neeve.aep.event.AepEngineStoppedEvent;
import com.neeve.config.Config;
import com.neeve.config.VMConfigurer;
import com.neeve.perf.aep.engine.state.Repository;
import com.neeve.perf.serialization.Driver;
import com.neeve.perf.serialization.Provider;
import com.neeve.perf.serialization.rumi.xbuf2.Car;
import com.neeve.rog.IRogMessage;
import com.neeve.server.embedded.EmbeddedXVM;
import com.neeve.server.config.SrvConfigDescriptor;
import com.neeve.server.app.annotations.AppHAPolicy;
import com.neeve.server.app.annotations.AppInjectionPoint;
import com.neeve.server.app.annotations.AppStateFactoryAccessor;
import com.neeve.sma.MessageView;
import com.neeve.util.UtlConstants;
import com.neeve.util.UtlTailoring;
import com.neeve.util.UtlTime;

@AppHAPolicy(value = AepEngine.HAPolicy.StateReplication)
final public class SRProcessor extends Processor {
    final private static Object mainThreadShutdownSynchronizer = new Object();
    final private Provider<Car> _provider;
    final private int _count;
    final private boolean _emptyMessage;
    private static boolean _engineStopped;

    private SRProcessor() throws Exception {
        _provider = (Provider<Car>)Driver.getProvider(System.getProperty(ConfigProperties.PROP_DRIVER_TEST_ENCODING));
        _count = Integer.valueOf(System.getProperty(ConfigProperties.PROP_DRIVER_TEST_COUNT));
        _emptyMessage = Boolean.valueOf(System.getProperty(ConfigProperties.PROP_DRIVER_TEST_EMPTY_MESSAGE));
    }

	@AppStateFactoryAccessor
    final public IAepApplicationStateFactory getStateFactory() {
        return new IAepApplicationStateFactory() {
            @Override
            final public Repository createState(MessageView view) {
                return Repository.create();
            }
        };
    }

    @EventHandler
    final public void onMessage(final Car inMessage, final Repository repository) throws Exception {
        // read inbound message
        _provider.decode(inMessage);

        // prepare outbound message
        final IRogMessage outMessage = _provider.create(!_emptyMessage);

        // send outbound
        outMessage.setPostWireTs(inMessage.getPostWireTs());
        _messageSender.sendMessage(1, outMessage);
    }

    @EventHandler
    final public void onEngineStopped(AepEngineStoppedEvent event) {
        synchronized(mainThreadShutdownSynchronizer) {
            _engineStopped = true;
            mainThreadShutdownSynchronizer.notifyAll();
        }
    }

    private static void printUsage() {
        System.err.println("Usage SRProcessor");
        System.err.println("--------------------------------------------General Parameters------------------------------------------------------");
        System.err.println(" [{-l, --encoding} encoding type of messages to inject]");
        System.err.println("   The encoding type of messages to inject [xbuf2.serial | xbuf2.random] (default=xbuf2.serial)");
        System.err.println(" [{-c, --count} number of messages to inject]");
        System.err.println("   Number of messages to inject (default=10,000,000)");
        System.err.println(" [{-t, --warmupTime} Warmup time]");
        System.err.println("   Warmup time, in seconds, for calculation of throughput stats (default=2 (2 seconds))");
        System.err.println(" [{-r, --rate} commit rate]");
        System.err.println("   Rate at which to send messages (default=100,000 (unlimited))");
        System.err.println(" [{-E, --emptyMessage} do not populate the inbound and outbound messages]");
        System.err.println("   Specifies that inbound and outbound message should not be populated (false))");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-a, --noLatencyWrites} don't write latencies to a file");
        System.err.println("   Indicates that latencies should not be written to a file (default=false)");
        System.err.println(" [{-b, --printIntervalStats} print interval latency stats");
        System.err.println("   Indicates that latencies stats should be printed on a periodic basis in addition to at the end (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-O, --outputFile} the file to write the result to");
        System.err.println("   The excel file to write the results to (default=null)");
        System.err.println(" [{-C, --outputCell} the cell in the output file to write the result to");
        System.err.println("   Specifies the cell, in <ROW>-<COL> format, in the result excel file where the result should be written (default=null)");
        System.err.println(" [{-T, --outputThroughput} write throughput result instead of latencies");
        System.err.println("   Specifies that the throughput result of the test should be written instead of latencies (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-j, --injectorCPUAffinityMask} CPU affinity mask of the injecting thread");
        System.err.println("   Sets the CPU affinity mask of the injecting thread (default=null)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-s, --server} Launch in a Rumi server]");
        System.err.println("   If specified, then the processor launches an embedded Rumi server to launch and manage the service. (default=false)");
        System.err.println("");
        System.err.println("---------------------------------------Engine Multiplexer Parameters------------------------------------------------");
        System.err.println(" [{-g, --muxQueueDepth} Depth of the engine event multiplexer queue");
        System.err.println("   Sets the depth of the engine event multiplexer queue (default=1024)");
        System.err.println(" [{-y, --muxCPUAffinityMask} CPU affinity mask of the engine event multiplexer thread");
        System.err.println("   Sets the CPU affinity mask of the engine event multiplexer thread (default=null)");
        System.err.println("");
        System.err.println("-------------------------------------------Message Bus Parameters---------------------------------------------------");
        System.err.println(" [{-u, --busDetachedSend} sends outbound messages in a separate thread]");
        System.err.println("   Switches on detached sender in the bus connection manager (concurrent write in a separate thread) on or off (default=false)");
        System.err.println(" [{-n, --busDetachedSendQueueDepth} Depth of the messaging bus detached send queue");
        System.err.println("   Sets the depth of the messaging bus detached send queue (default=1024)");
        System.err.println(" [{-o, --busDetachedSendCPUAffinityMask} CPU affinity mask of the bus detached sender thread");
        System.err.println("   Sets the CPU affinity mask of the bus detached sender thread (default=null)");
        System.err.println("");
        System.err.println("-----------------------------------------Store Persister Parameters-------------------------------------------------");
        System.err.println(" [{-e, --enablePersistence} whether to disable store persistence]");
        System.err.println("   Specifies whether persistence is disabled (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-k, --persisterLogLocation} the directory where to create the store log]");
        System.err.println("   Specifies the directory where the store log should be created (default=\".\")");
        System.err.println(" [{-i, --persisterInitialLogLength} the preallocated length of the store log]");
        System.err.println("   Specifies the preallocated length (in gigabytes) of the store log (default=20)");
        System.err.println(" [{-z, --persisterZeroOutInitial} zeroes out the store log. only applies if --initialLength is specified and > 0]");
        System.err.println("   Specifies whether to zero out the preallocated length of the store log (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-w, --persisterWriteBufferSize} specifies the store log write buffer size]");
        System.err.println("   Specifies, in bytes, the store log's write buffer size (default=8192)");
        System.err.println(" [{-m, --persisterFlushUsingMappedMemory} whether to flush using a memory mapped region of the store log");
        System.err.println("   Specifies whether to use a memory mapped region of the store log to perform flush operations (default=false)");
        System.err.println(" [{-f, --persisterFlushOnCommit} whether to flush the store log on every commit]");
        System.err.println("   Specifies whether the in memory cached entries of the log are forcibly flushed on every commit (default=false)");
        System.err.println("   <Note: A flush does not imply sync>");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-d, --persisterDetached} run store persister in detached mode]");
        System.err.println("   Switches on detached writes to the store log (concurrent write in a separate thread) on or off (default=false)");
        System.err.println(" [{-q, --persisterQueueDepth} queue depth for detached writes]");
        System.err.println("   Specifies the queue depth for detached writes (default=1024)");
        System.err.println("   <This option only applies to detached writes>");
        System.err.println(" [{-x, --persisterWriterCPUAffinityMask} writer thread CPU affinity mask for detached writes]");
        System.err.println("   Specifies the writer thread CPU affinity mask for detached thread. (default=null)");
        System.err.println("   <This option only applies to detached writes>");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-t, --persisterReadBufferSize} specifies the log read buffer size]");
        System.err.println("   Specifies, in bytes, the log's read buffer size (default=8192)");
        System.err.println(" [{-p, --persisterPageSize} specifies the disk subsystem page size]");
        System.err.println("   Specifies (in bytes) the page size to use when reading/writing from/to disk (default=8192)");
        System.err.println("");
        System.err.println("-----------------------------------------Store Clustering Parameters------------------------------------------------");
        System.err.println(" [{-v, --enableClustering} whether to disable store clustering]");
        System.err.println("   Specifies whether clustering is disabled (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-U, --clusteringDiscoveryLocalIfAddr} specifies the local interface to use for cluster discovery]");
        System.err.println("   Specifies the local interface to use for cluster discovery (default=0.0.0.0)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-I, --clusteringLocalIfAddr} specifies the local interface to use for cluster replication]");
        System.err.println("   Specifies the local interface to use for cluster replication (default=0.0.0.0)");
        System.err.println(" [{-P, --clusteringLocalPort} specifies the local port to use for cluster replication]");
        System.err.println("   Specifies the local port to use for cluster replication (default=0)");
        System.err.println(" [{-V, --clusteringLinkReaderCPUAffinityMask} cluster replication link reader thread CPU affinity mask]");
        System.err.println("   Specifies the cluster replication link reader thread CPU affinity mask. (default=null)");
        System.err.println(" [{-W, --clusteringLinkSpinRead} whether the cluster replication link should perform spinning reads]");
        System.err.println("   Specifies whether the network reads performed by the cluster replication link should spin instead of block. (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-S, --clusteringDetachedSend} run store replicator in detached send mode]");
        System.err.println("   Switches on detached send for cluster replication (concurrent send in a separate thread) on or off (default=false)");
        System.err.println(" [{-Q, --clusteringDetachedSendQueueDepth} queue depth for detached cluster replication send]");
        System.err.println("   Specifies the queue depth for detached cluster replication send (default=1024)");
        System.err.println("   <This option only applies to detached cluster replication send>");
        System.err.println(" [{-A, --clusteringDetachedSenderCPUAffinityMask} writer thread CPU affinity mask for the cluster replication detached sender]");
        System.err.println("   Specifies the cluster replication detached sender thread CPU affinity mask. (default=null)");
        System.err.println("   <This option only applies to detached cluster replication send>");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-D, --clusteringDetachedDispatch} run store replicator in detached dispatch mode]");
        System.err.println("   Switches on detached dispatch for cluster replication (concurrent dispatch in a separate thread) on or off (default=false)");
        System.err.println(" [{-R, --clusteringDetachedDispatchQueueDepth} queue depth for detached cluster replication dispatch]");
        System.err.println("   Specifies the queue depth for detached cluster replication dispatch (default=1024)");
        System.err.println("   <This option only applies to detached cluster replication dispatch>");
        System.err.println(" [{-B, --clusteringDetachedDispatcherCPUAffinityMask} writer thread CPU affinity mask for the cluster replication detached dispatcher]");
        System.err.println("   Specifies the cluster replication detached dispatcher thread CPU affinity mask. (default=null)");
        System.err.println("   <This option only applies to detached cluster replication dispatch>");
        System.err.println("");
        System.err.println("----------------------------------------------------Help------------------------------------------------------------");
        System.err.println(" [{-h, --help} print this help string]");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
    }

    final public static void main(final String args[]) {
        final CmdLineParser parser = new CmdLineParser();

        // driver options
        final CmdLineParser.Option encodingOption = parser.addStringOption('l', "encoding");
        final CmdLineParser.Option countOption = parser.addIntegerOption('c', "count");
        final CmdLineParser.Option warmupTimeOption = parser.addIntegerOption('t', "warmupTime");
        final CmdLineParser.Option rateOption = parser.addIntegerOption('r', "rate");
        final CmdLineParser.Option emptyMessageOption = parser.addBooleanOption('E', "emptyMessage");
        final CmdLineParser.Option noLatencyWritesOption = parser.addBooleanOption('a', "noLatencyWrites");
        final CmdLineParser.Option printIntervalStatsOption = parser.addBooleanOption('b', "printIntervalStats");
        final CmdLineParser.Option injectorCPUAffinityMaskOption = parser.addStringOption('j', "injectorCPUAffinityMask");

        // output options
        final CmdLineParser.Option outputFileOption = parser.addStringOption('O', "outputFile");
        final CmdLineParser.Option outputCellOption = parser.addStringOption('C', "outputCell");
        final CmdLineParser.Option outputThroughputOption = parser.addBooleanOption('T', "outputThroughput");

        // containerization options
        final CmdLineParser.Option serverOption = parser.addBooleanOption('s', "server");

        // engine event mux options
        final CmdLineParser.Option muxQueueDepthOption = parser.addIntegerOption('g', "muxQueueDepth");
        final CmdLineParser.Option muxCPUAffinityMaskOption = parser.addStringOption('y', "muxCPUAffinityMask");

        // message bus options
        final CmdLineParser.Option busDetachedSendOption = parser.addStringOption('u', "busDetachedSend");
        final CmdLineParser.Option busDetachedSendQueueDepthOption = parser.addIntegerOption('n', "busDetachedSendQueueDepth");
        final CmdLineParser.Option busDetachedSendCPUAffinityMaskOption = parser.addStringOption('o', "busDetachedSendCPUAffinityMask");

        // store persister related options
        final CmdLineParser.Option enablePersistenceOption = parser.addBooleanOption('e', "enablePersistence");
        final CmdLineParser.Option persisterLogLocationOption = parser.addStringOption('k', "persisterLogLocation");
        final CmdLineParser.Option persisterInitialLogLengthOption = parser.addIntegerOption('i', "persisterInitialLogLength");
        final CmdLineParser.Option persisterZeroOutInitialOption = parser.addBooleanOption('z', "persisterZeroOutInitial");
        final CmdLineParser.Option persisterWriteBufferSizeOption = parser.addIntegerOption('w', "persisterWriteBufferSize");
        final CmdLineParser.Option persisterFlushUsingMappedMemoryOption = parser.addBooleanOption('m', "persisterFlushUsingMappedMemory");
        final CmdLineParser.Option persisterFlushOnCommitOption = parser.addBooleanOption('f', "persisterFlushOnCommit");
        final CmdLineParser.Option persisterDetachedOption = parser.addStringOption('d', "persisterDetached");
        final CmdLineParser.Option persisterQueueDepthOption = parser.addIntegerOption('q', "persisterQueueDepth");
        final CmdLineParser.Option persisterWriterCPUAffinityMaskOption = parser.addStringOption('x', "persisterWriterCPUAffinityMask");
        final CmdLineParser.Option persisterReadBufferSizeOption = parser.addIntegerOption('t', "persisterReadBufferSize");
        final CmdLineParser.Option persisterPageSizeOption = parser.addIntegerOption('p', "persisterPageSize");

        // store clustering related options
        final CmdLineParser.Option enableClusteringOption = parser.addBooleanOption('v', "enableClustering");
        final CmdLineParser.Option clusteringDiscoveryLocalIfAddrOption = parser.addStringOption('U', "clusteringDiscoveryLocalIfAddr");
        final CmdLineParser.Option clusteringLocalIfAddrOption = parser.addStringOption('I', "clusteringLocalIfAddr");
        final CmdLineParser.Option clusteringLocalPortOption = parser.addStringOption('P', "clusteringLocalPort");
        final CmdLineParser.Option clusteringLinkReaderCPUAffinityMaskOption = parser.addStringOption('V', "clusteringLinkReaderCPUAffinityMask");
        final CmdLineParser.Option clusteringLinkSpinReadOption = parser.addStringOption('W', "clusteringLinkSpinRead");
        final CmdLineParser.Option clusteringDetachedSendOption = parser.addBooleanOption('S', "clusteringDetachedSend");
        final CmdLineParser.Option clusteringDetachedSendQueueDepthOption = parser.addIntegerOption('Q', "clusteringDetachedSendQueueDepth");
        final CmdLineParser.Option clusteringDetachedSenderCPUAffinityMaskOption = parser.addStringOption('A', "clusteringDetachedSenderCPUAffinityMask");
        final CmdLineParser.Option clusteringDetachedDispatchOption = parser.addBooleanOption('S', "clusteringDetachedDispatch");
        final CmdLineParser.Option clusteringDetachedDispatchQueueDepthOption = parser.addIntegerOption('Q', "clusteringDetachedDispatchQueueDepth");
        final CmdLineParser.Option clusteringDetachedDispatcherCPUAffinityMaskOption = parser.addStringOption('A', "clusteringDetachedDispatcherCPUAffinityMask");

        // help 
        final CmdLineParser.Option helpOption = parser.addBooleanOption('h', "help");

        try {
            // parse
            parser.parse(args);
            if (!((Boolean)parser.getOptionValue(helpOption, false))) {
                // update environment to localize the config
                // ...driver
                System.setProperty(ConfigProperties.PROP_DRIVER_TEST_ENCODING, (String)parser.getOptionValue(encodingOption, "xbuf2.serial"));
                System.setProperty(ConfigProperties.PROP_DRIVER_TEST_COUNT, String.valueOf((Integer)parser.getOptionValue(countOption, 10000000)));
                System.setProperty(ConfigProperties.PROP_DRIVER_TEST_WARMUP_TIME, String.valueOf((Integer)parser.getOptionValue(warmupTimeOption, 2)));
                System.setProperty(ConfigProperties.PROP_DRIVER_TEST_RATE, String.valueOf((Integer)parser.getOptionValue(rateOption, 100000)));
                System.setProperty(ConfigProperties.PROP_DRIVER_TEST_EMPTY_MESSAGE, (Boolean)parser.getOptionValue(emptyMessageOption, false) ? "true" : "false");
                System.setProperty(ConfigProperties.PROP_DRIVER_LW_NOWRITE, ((Boolean)parser.getOptionValue(noLatencyWritesOption, false)) ? "true" : "false");
                System.setProperty(ConfigProperties.PROP_DRIVER_LW_PRINT_INTERVAL_STATS, (Boolean)parser.getOptionValue(printIntervalStatsOption, false) ? "true" : "false");
                final String injectorCPUAffinityMask = (String)parser.getOptionValue(injectorCPUAffinityMaskOption, null);
                if (injectorCPUAffinityMask != null) {
                    System.setProperty(ConfigProperties.PROP_DRIVER_INJECTOR_CPU_AFFINITY_MASK, injectorCPUAffinityMask);
                }

                // ...output
                String val = (String)parser.getOptionValue(outputFileOption, null);
                if (val != null) {
                    System.setProperty(ConfigProperties.PROP_OUTPUT_FILE, val);
                }
                val = (String)parser.getOptionValue(outputCellOption, null);
                if (val != null) {
                    System.setProperty(ConfigProperties.PROP_OUTPUT_CELL, val);
                }
                System.setProperty(ConfigProperties.PROP_OUTPUT_THROUGHPUT, (Boolean)parser.getOptionValue(outputThroughputOption, false) ? "true" : "false");

                // ...multiplexer
                System.setProperty(ConfigProperties.PROP_MUX_QUEUE_DEPTH, String.valueOf((Integer)parser.getOptionValue(muxQueueDepthOption, 1024)));
                final String muxCPUAffinityMask = (String)parser.getOptionValue(muxCPUAffinityMaskOption, null);
                if (muxCPUAffinityMask != null) {
                    System.setProperty(ConfigProperties.PROP_MUX_CPU_AFFINITY_MASK, muxCPUAffinityMask);
                }

                // ...message bus
                String busDetachedSend;
                if ((busDetachedSend = (String)parser.getOptionValue(busDetachedSendOption, null)) != null) {
                    System.setProperty("x.apps.processor.messaging.buses.processor.detachedSend.enabled", busDetachedSend.equalsIgnoreCase("true") ? "true" : "false");
                }
                System.setProperty(ConfigProperties.PROP_BUS_DETACHED_SEND_QUEUE_DEPTH, String.valueOf((Integer)parser.getOptionValue(busDetachedSendQueueDepthOption, 1024)));
                final String busDetachedSendCPUAffinityMask = (String)parser.getOptionValue(busDetachedSendCPUAffinityMaskOption, null);
                if (busDetachedSendCPUAffinityMask != null) {
                    System.setProperty(ConfigProperties.PROP_BUS_DETACHED_SEND_QUEUE_DRAINER_CPU_AFFINITY_MASK, busDetachedSendCPUAffinityMask);
                }

                // ...persister
                final boolean enablePersistence = (Boolean)parser.getOptionValue(enablePersistenceOption, false);
                System.setProperty(ConfigProperties.PROP_PERSISTENCE_ENABLED, enablePersistence ? "true" : "false");
                System.setProperty(ConfigProperties.PROP_PERSISTENCE_LOG_LOCATION, (String)parser.getOptionValue(persisterLogLocationOption, "."));
                System.setProperty(ConfigProperties.PROP_PERSISTENCE_INITIAL_LOG_LENGTH, String.valueOf(parser.getOptionValue(persisterInitialLogLengthOption, 20)));
                System.setProperty(ConfigProperties.PROP_PERSISTENCE_ZERO_OUT_INITIAL, ((Boolean)parser.getOptionValue(persisterZeroOutInitialOption, false)) ? "true" : "false");
                System.setProperty(ConfigProperties.PROP_PERSISTENCE_WRITE_BUFFER_SIZE, String.valueOf(parser.getOptionValue(persisterWriteBufferSizeOption, 8192)));
                System.setProperty(ConfigProperties.PROP_PERSISTENCE_FLUSH_USING_MAPPED_MEMORY, ((Boolean)parser.getOptionValue(persisterFlushUsingMappedMemoryOption, false)) ? "true" : "false");
                System.setProperty(ConfigProperties.PROP_PERSISTENCE_FLUSH_ON_COMMIT, ((Boolean)parser.getOptionValue(persisterFlushOnCommitOption, false)) ? "true" : "false");
                String persisterDetached;
                if ((persisterDetached = (String)parser.getOptionValue(persisterDetachedOption, null)) != null) {
                    System.setProperty("x.apps.processor.storage.persistence.detachedPersist.enabled", persisterDetached.equalsIgnoreCase("true") ? "true" : "false");
                }
                System.setProperty(ConfigProperties.PROP_PERSISTENCE_DETACHED_QUEUE_DEPTH, String.valueOf(parser.getOptionValue(persisterQueueDepthOption, 1024)));
                final String persisterWriterCPUAffinityMask = (String)parser.getOptionValue(persisterWriterCPUAffinityMaskOption, null);
                if (persisterWriterCPUAffinityMask != null) {
                    System.setProperty(ConfigProperties.PROP_PERSISTENCE_DETACHED_QUEUE_DRAINER_CPU_AFFINITY_MASK, persisterWriterCPUAffinityMask);
                }
                System.setProperty(ConfigProperties.PROP_PERSISTENCE_READ_BUFFER_SIZE, String.valueOf(parser.getOptionValue(persisterReadBufferSizeOption, 8192)));
                System.setProperty(ConfigProperties.PROP_PERSISTENCE_PAGE_SIZE, String.valueOf(parser.getOptionValue(persisterPageSizeOption, 4096)));

                // ... clustering
                final boolean enableClustering = (Boolean)parser.getOptionValue(enableClusteringOption, false);
                System.setProperty(ConfigProperties.PROP_CLUSTERING_ENABLED, enableClustering ? "true" : "false");
                System.setProperty(ConfigProperties.PROP_CLUSTERING_DISCOVERY_LOCAL_IF_ADDR, (String)parser.getOptionValue(clusteringDiscoveryLocalIfAddrOption, "0.0.0.0"));
                System.setProperty(ConfigProperties.PROP_CLUSTERING_LOCAL_IF_ADDR, (String)parser.getOptionValue(clusteringLocalIfAddrOption, "0.0.0.0"));
                System.setProperty(ConfigProperties.PROP_CLUSTERING_LOCAL_PORT, (String)parser.getOptionValue(clusteringLocalPortOption, "0"));
                final String clusteringLinkReaderCPUAffinityMask = (String)parser.getOptionValue(clusteringLinkReaderCPUAffinityMaskOption, null);
                if (clusteringLinkReaderCPUAffinityMask != null) {
                    System.setProperty(ConfigProperties.PROP_CLUSTERING_LINK_READER_CPU_AFFINITY_MASK, clusteringLinkReaderCPUAffinityMask);
                }
                String clusteringLinkSpinRead;
                if ((clusteringLinkSpinRead = (String)parser.getOptionValue(clusteringLinkSpinReadOption, null)) != null) {
                    System.setProperty(ConfigProperties.PROP_CLUSTERING_LINK_PARAMS, "eagerread=" + (clusteringLinkSpinRead.equalsIgnoreCase("true") ? "true" : "false"));
                }
                boolean clusteringDetachedSend = (Boolean)parser.getOptionValue(clusteringDetachedSendOption, false);
                System.setProperty(ConfigProperties.PROP_CLUSTERING_DETACHED_SEND, clusteringDetachedSend ? "true" : "false");
                System.setProperty(ConfigProperties.PROP_CLUSTERING_DETACHED_SEND_QUEUE_DEPTH, String.valueOf(parser.getOptionValue(clusteringDetachedSendQueueDepthOption, 1024)));
                final String clusteringDetachedSenderCPUAffinityMask = (String)parser.getOptionValue(clusteringDetachedSenderCPUAffinityMaskOption, null);
                if (clusteringDetachedSenderCPUAffinityMask != null) {
                    System.setProperty(ConfigProperties.PROP_CLUSTERING_DETACHED_SEND_QUEUE_DRAINER_CPU_AFFINITY_MASK, clusteringDetachedSenderCPUAffinityMask);
                }
                boolean clusteringDetachedDispatch = (Boolean)parser.getOptionValue(clusteringDetachedDispatchOption, false);
                System.setProperty(ConfigProperties.PROP_CLUSTERING_DETACHED_DISPATCH, clusteringDetachedDispatch ? "true" : "false");
                System.setProperty(ConfigProperties.PROP_CLUSTERING_DETACHED_DISPATCH_QUEUE_DEPTH, String.valueOf(parser.getOptionValue(clusteringDetachedDispatchQueueDepthOption, 1024)));
                final String clusteringDetachedDispatcherCPUAffinityMask = (String)parser.getOptionValue(clusteringDetachedDispatcherCPUAffinityMaskOption, null);
                if (clusteringDetachedDispatcherCPUAffinityMask != null) {
                    System.setProperty(ConfigProperties.PROP_CLUSTERING_DETACHED_DISPATCH_QUEUE_DRAINER_CPU_AFFINITY_MASK, clusteringDetachedDispatcherCPUAffinityMask);
                }
                
                // ... storage
                if (enablePersistence || enableClustering) {
                    System.setProperty(ConfigProperties.PROP_STORAGE_ENABLED, "true");
                }

                // ...prompt top start
                if (enableClustering) {
                    System.setProperty(ConfigProperties.PROP_DRIVER_PROMPT_TO_START, "true");
                }

                System.out.println("");
                System.out.println("Parameters");
                System.out.println("...Driver {");
                System.out.println("......encoding=" + System.getProperty(ConfigProperties.PROP_DRIVER_TEST_ENCODING));
                System.out.println("......count=" + System.getProperty(ConfigProperties.PROP_DRIVER_TEST_COUNT));
                System.out.println("......warmupTime=" + System.getProperty(ConfigProperties.PROP_DRIVER_TEST_WARMUP_TIME));
                System.out.println("......rate=" + System.getProperty(ConfigProperties.PROP_DRIVER_TEST_RATE));
                System.out.println("......emptyMessage=" + System.getProperty(ConfigProperties.PROP_DRIVER_TEST_EMPTY_MESSAGE));
                System.out.println("......noLatencyWrites=" + System.getProperty(ConfigProperties.PROP_DRIVER_LW_NOWRITE));
                System.out.println("......printIntervalStats=" + System.getProperty(ConfigProperties.PROP_DRIVER_LW_PRINT_INTERVAL_STATS));
                System.out.println("......injectorCPUAffinityMask=" + System.getProperty(ConfigProperties.PROP_DRIVER_INJECTOR_CPU_AFFINITY_MASK));
                System.out.println("...}");
                System.out.println("...Output {");
                System.out.println("......file=" + System.getProperty(ConfigProperties.PROP_OUTPUT_FILE));
                System.out.println("......cell=" + System.getProperty(ConfigProperties.PROP_OUTPUT_CELL));
                System.out.println("......throughput=" + System.getProperty(ConfigProperties.PROP_OUTPUT_THROUGHPUT));
                System.out.println("...}");
                System.out.println("...Containerization {");
                final boolean launchInServer = (Boolean)parser.getOptionValue(serverOption, false);
                System.out.println("......launchInServer=" + launchInServer);
                System.out.println("...}");
                System.out.println("...Engine Event Mux {");
                System.out.println("......queueDepth=" + System.getProperty(ConfigProperties.PROP_MUX_QUEUE_DEPTH));
                System.out.println("......muxCPUAffinityMask=" + System.getProperty(ConfigProperties.PROP_MUX_CPU_AFFINITY_MASK));
                System.out.println("...}");
                System.out.println("...Message Bus {");
                System.out.println("......detached = " + (busDetachedSend != null ? busDetachedSend : "<system>"));
                System.out.println(".........queueDepth=" + System.getProperty(ConfigProperties.PROP_BUS_DETACHED_SEND_QUEUE_DEPTH));
                System.out.println(".........senderCPUAffinityMask=" + System.getProperty(ConfigProperties.PROP_BUS_DETACHED_SEND_QUEUE_DRAINER_CPU_AFFINITY_MASK));
                System.out.println("...}");
                System.out.println("...Store Persister {");
                System.out.println("......enabled=" + enablePersistence);
                if (enablePersistence) {
                    System.out.println("......initialLogLength=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_INITIAL_LOG_LENGTH));
                    System.out.println("......zeroOutInitial=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_ZERO_OUT_INITIAL));
                    System.out.println("......flushUsingMappedMemory=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_FLUSH_USING_MAPPED_MEMORY));
                    System.out.println("......writeBufferSize=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_WRITE_BUFFER_SIZE));
                    System.out.println("......flushOnCommit=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_FLUSH_ON_COMMIT));
                    System.out.println("......detached = " + (persisterDetached != null ? persisterDetached : "<system>"));
                    System.out.println(".........queueDepth=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_DETACHED_QUEUE_DEPTH));
                    System.out.println(".........writerCPUAffinityMask=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_DETACHED_QUEUE_DRAINER_CPU_AFFINITY_MASK));
                    System.out.println("......readBufferSize=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_READ_BUFFER_SIZE));
                    System.out.println("......pageSize=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_PAGE_SIZE));
                }
                System.out.println("...}");
                System.out.println("...Store Replicator {");
                System.out.println("......enabled=" + enableClustering);
                if (enableClustering) {
                    System.out.println("......discoveryLocalIfAddr=" + System.getProperty(ConfigProperties.PROP_CLUSTERING_DISCOVERY_LOCAL_IF_ADDR));
                    System.out.println("......link");
                    System.out.println(".........localIfAddr=" + System.getProperty(ConfigProperties.PROP_CLUSTERING_LOCAL_IF_ADDR));
                    System.out.println(".........localPort=" + System.getProperty(ConfigProperties.PROP_CLUSTERING_LOCAL_PORT));
                    System.out.println(".........readerCPUAffinityMask=" + System.getProperty(ConfigProperties.PROP_CLUSTERING_LINK_READER_CPU_AFFINITY_MASK));
                    System.out.println(".........linkParams=" + System.getProperty(ConfigProperties.PROP_CLUSTERING_LINK_PARAMS));
                    System.out.println("......detachedSend= " + System.getProperty(ConfigProperties.PROP_CLUSTERING_DETACHED_SEND));
                    System.out.println(".........queueDepth=" + System.getProperty(ConfigProperties.PROP_CLUSTERING_DETACHED_SEND_QUEUE_DEPTH));
                    System.out.println(".........senderCPUAffinityMask=" + System.getProperty(ConfigProperties.PROP_CLUSTERING_DETACHED_SEND_QUEUE_DRAINER_CPU_AFFINITY_MASK));
                    System.out.println("......detachedDispatch= " + System.getProperty(ConfigProperties.PROP_CLUSTERING_DETACHED_DISPATCH));
                    System.out.println(".........queueDepth=" + System.getProperty(ConfigProperties.PROP_CLUSTERING_DETACHED_DISPATCH_QUEUE_DEPTH));
                    System.out.println(".........dispatcherCPUAffinityMask=" + System.getProperty(ConfigProperties.PROP_CLUSTERING_DETACHED_DISPATCH_QUEUE_DRAINER_CPU_AFFINITY_MASK));
                }
                System.out.println("...}");
                System.out.println("");

                // enable affinitization
                if (injectorCPUAffinityMask != null || 
                    muxCPUAffinityMask != null || 
                    (busDetachedSend != null && busDetachedSend.equalsIgnoreCase("true") && busDetachedSendCPUAffinityMask != null) || 
                    (enablePersistence && persisterDetached != null && persisterDetached.equalsIgnoreCase("true") && persisterWriterCPUAffinityMask != null) || 
                    (enableClustering && clusteringLinkSpinRead != null && clusteringLinkSpinRead.equalsIgnoreCase("true") && clusteringLinkReaderCPUAffinityMask != null) ||
                    (enableClustering && clusteringDetachedSend && clusteringDetachedSenderCPUAffinityMask != null) || 
                    (enableClustering &&  clusteringDetachedDispatch && clusteringDetachedDispatcherCPUAffinityMask != null)) {
                    System.setProperty(UtlConstants.THREAD_ENABLECPUAFFINITYMASKS_PROPNAME, "true");
                    if (injectorCPUAffinityMask == null || 
                        muxCPUAffinityMask == null || 
                        (busDetachedSend != null && busDetachedSend.equalsIgnoreCase("true") && busDetachedSendCPUAffinityMask == null) || 
                        (enablePersistence && persisterDetached != null && persisterDetached.equalsIgnoreCase("true") && persisterWriterCPUAffinityMask == null) || 
                        (enableClustering && clusteringLinkSpinRead != null && clusteringLinkSpinRead.equalsIgnoreCase("true") && clusteringLinkReaderCPUAffinityMask == null) ||
                        (enableClustering && clusteringDetachedSend && clusteringDetachedSenderCPUAffinityMask == null) || 
                        (enableClustering &&  clusteringDetachedDispatch && clusteringDetachedDispatcherCPUAffinityMask == null)) {
                        System.out.println("");
                        System.out.println("*****************************************************************************");
                        System.out.println("                               WARNING!!!                                    ");
                        System.out.println("  At least one CPU affinity mask is set due to which CPU affinitization has  ");
                        System.out.println("  been implicitly enabled. However, one or more CPU affinity masks have not  ");
                        System.out.println("  been configured. This can result in significantly degraded performance.    ");
                        System.out.println("  Please ensure either none or CPU affinity masks for all threads that are   ");
                        System.out.println("  affinitizable have been set.                                               ");
                        System.out.println("****************************************************************************");
                        System.out.println("");
                    }
                }

                // load config
                VMConfigurer.configure(new File(Paths.get(Config.getRootDirectory().toString(), "conf", "config.xml").toString()), UtlTailoring.ENV_SUBSTITUTION_RESOLVER);

                // how we start the app depends on whether telemtry is enabled or not
                if (launchInServer) {
                    // start the server
                    SrvConfigDescriptor serverDescriptor = SrvConfigDescriptor.load("processor-1a");
                    EmbeddedXVM server = EmbeddedXVM.create(serverDescriptor);
                    server.start();

                    // the server will initialize the config environment, use the config to 
                    // create the processor, create and inject the engine and sender into the 
                    // processor and start the engine (essentially the server uses config to 
                    // do all of the steps below)
                }
                else {
                    // create processor
                    final SRProcessor processor = new SRProcessor();

                    // create the engine
                    final AepMessageSender sender = AepMessageSender.create();
                    final AepEngineDescriptor engineDescriptor = AepEngineDescriptor.load("processor");
                    engineDescriptor.setHAPolicy(AepEngine.HAPolicy.StateReplication);
                    final AepEngine engine = AepEngine.create(engineDescriptor, 
                                                              processor.getStateFactory(),
                                                              new HashSet<Object>(Arrays.asList(new Object[] { processor })), 
                                                              null,
                                                              new HashSet<AepMessageSender>(Arrays.asList(new AepMessageSender[] { sender })), 
                                                              null);

                    // prepare the processor
                    processor.setMessageSender(sender);
                    processor.setEngine(engine);

                    // start engine
                    engine.start();
                }

                // block and wait. the remainder of the app is driven by inbound messages
                // driven by the processor's engine
                synchronized(mainThreadShutdownSynchronizer) {
                    while (!_engineStopped) {
                        mainThreadShutdownSynchronizer.wait();
                    }
                }
            }
            else {
                printUsage();
            }
        }
        catch (Throwable e) {
            e.printStackTrace();
        }
    }
}

