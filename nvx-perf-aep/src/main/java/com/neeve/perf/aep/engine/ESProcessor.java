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
import com.neeve.aep.annotations.EventHandler;
import com.neeve.config.Config;
import com.neeve.config.VMConfigurer;
import com.neeve.perf.serialization.MessageFactory;
import com.neeve.perf.serialization.rumi.xbuf2.Car;
import com.neeve.rog.IRogMessage;
import com.neeve.server.embedded.EmbeddedXVM;
import com.neeve.server.config.SrvConfigDescriptor;
import com.neeve.server.app.annotations.AppHAPolicy;
import com.neeve.server.app.annotations.AppInjectionPoint;
import com.neeve.util.UtlConstants;
import com.neeve.util.UtlTailoring;
import com.neeve.util.UtlTime;

@AppHAPolicy(value = AepEngine.HAPolicy.EventSourcing)
final public class ESProcessor {
    final private MessageFactory _messageFactory;
    private AepEngine _engine;
    private AepMessageSender _messageSender;

    private ESProcessor() {
        _messageFactory = new MessageFactory(System.getProperty(ConfigProperties.PROP_DRIVER_TEST_ENCODING));
    }

	@AppInjectionPoint
	final public void setEngine(AepEngine engine) {
		_engine = engine;
	}

	@AppInjectionPoint
	final public void setMessageSender(AepMessageSender messageSender) {
		_messageSender = messageSender;
	}

    @EventHandler
    final public void onMessage(final Car inMessage) throws Exception {
        // prepare outbound message
        final IRogMessage outMessage = (IRogMessage)_messageFactory.createCar(true);

        // send outbound
        outMessage.setPostWireTs(inMessage.getPostWireTs());
        _messageSender.sendMessage(1, outMessage);
    }

    private static void printUsage() {
        System.err.println("Usage ESProcessor");
        System.err.println("--------------------------------------------General Parameters------------------------------------------------------");
        System.err.println(" [{-l, --encoding} encoding type of messages to inject]");
        System.err.println("   The encoding type of messages to inject [xbuf2.serial | xbuf2.random] (default=xbuf2.serial)");
        System.err.println(" [{-c, --count} number of messages to inject]");
        System.err.println("   Number of messages to inject (default=10,000,000)");
        System.err.println(" [{-t, --warmupTime} Warmup time]");
        System.err.println("   Warmup time, in seconds, for calculation of throughput stats (default=2 (2 seconds))");
        System.err.println(" [{-r, --rate} commit rate]");
        System.err.println("   Rate at which to send messages (default=100,000 (unlimited))");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-a, --noLatencyWrites} don't write latencies to a file");
        System.err.println("   Indicates that latencies should not be written to a file (default=false)");
        System.err.println(" [{-b, --printIntervalStats} print interval latency stats");
        System.err.println("   Indicates that latencies stats should be printed on a periodic basis in addition to at the end (default=false)");
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
        System.err.println(" [{-y, --muxCPUAffinityMask} CPU affinity mask of the engine event multiiplexer thread");
        System.err.println("   Sets the CPU affinity mask of the engine event multiplexer thread (default=null)");
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
        final CmdLineParser.Option noLatencyWritesOption = parser.addBooleanOption('a', "noLatencyWrites");
        final CmdLineParser.Option printIntervalStatsOption = parser.addBooleanOption('b', "printIntervalStats");
        final CmdLineParser.Option injectorCPUAffinityMaskOption = parser.addStringOption('j', "injectorCPUAffinityMask");

        // containerization options
        final CmdLineParser.Option serverOption = parser.addBooleanOption('s', "server");

        // engine event mux options
        final CmdLineParser.Option muxQueueDepthOption = parser.addIntegerOption('g', "muxQueueDepth");
        final CmdLineParser.Option muxCPUAffinityMaskOption = parser.addStringOption('y', "muxCPUAffinityMask");

        // store persister related options
        final CmdLineParser.Option enablePersistenceOption = parser.addBooleanOption('e', "enablePersistence");
        final CmdLineParser.Option persisterLogLocationOption = parser.addStringOption('k', "persisterLogLocation");
        final CmdLineParser.Option persisterInitialLogLengthOption = parser.addIntegerOption('i', "persisterInitialLogLength");
        final CmdLineParser.Option persisterZeroOutInitialOption = parser.addBooleanOption('z', "persisterZeroOutInitial");
        final CmdLineParser.Option persisterWriteBufferSizeOption = parser.addIntegerOption('w', "persisterWriteBufferSize");
        final CmdLineParser.Option persisterFlushUsingMappedMemoryOption = parser.addBooleanOption('m', "persisterFlushUsingMappedMemory");
        final CmdLineParser.Option persisterFlushOnCommitOption = parser.addBooleanOption('f', "persisterFlushOnCommit");
        final CmdLineParser.Option detachedOption = parser.addBooleanOption('d', "persisterDetached");
        final CmdLineParser.Option persisterQueueDepthOption = parser.addIntegerOption('q', "persisterQueueDepth");
        final CmdLineParser.Option persisterWriterCPUAffinityMaskOption = parser.addStringOption('x', "persisterWriterCPUAffinityMask");
        final CmdLineParser.Option persisterReadBufferSizeOption = parser.addIntegerOption('t', "persisterReadBufferSize");
        final CmdLineParser.Option persisterPageSizeOption = parser.addIntegerOption('p', "persisterPageSize");

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
                System.setProperty(ConfigProperties.PROP_DRIVER_LW_NOWRITE, ((Boolean)parser.getOptionValue(noLatencyWritesOption, false)) ? "true" : "false");
                System.setProperty(ConfigProperties.PROP_DRIVER_LW_PRINT_INTERVAL_STATS, (Boolean)parser.getOptionValue(printIntervalStatsOption, false) ? "true" : "false");
                final String injectorCPUAffinityMask = (String)parser.getOptionValue(injectorCPUAffinityMaskOption, null);
                if (injectorCPUAffinityMask != null) {
                    System.setProperty(ConfigProperties.PROP_DRIVER_INJECTOR_CPU_AFFINITY_MASK, injectorCPUAffinityMask);
                }

                // ...multiplexer
                System.setProperty(ConfigProperties.PROP_MUX_QUEUE_DEPTH, String.valueOf((Integer)parser.getOptionValue(muxQueueDepthOption, 1024)));
                final String muxCPUAffinityMask = (String)parser.getOptionValue(muxCPUAffinityMaskOption, null);
                if (muxCPUAffinityMask != null) {
                    System.setProperty(ConfigProperties.PROP_MUX_CPU_AFFINITY_MASK, muxCPUAffinityMask);
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
                System.setProperty(ConfigProperties.PROP_PERSISTENCE_DETACHED, ((Boolean)parser.getOptionValue(detachedOption, false)) ? "true" : "false");
                System.setProperty(ConfigProperties.PROP_PERSISTENCE_DETACHED_QUEUE_DEPTH, String.valueOf(parser.getOptionValue(persisterQueueDepthOption, 1024)));
                final String persisterWriterCPUAffinityMask = (String)parser.getOptionValue(persisterWriterCPUAffinityMaskOption, null);
                if (persisterWriterCPUAffinityMask != null) {
                    System.setProperty(ConfigProperties.PROP_PERSISTENCE_DETACHED_QUEUE_DRAINER_CPU_AFFINITY_MASK, persisterWriterCPUAffinityMask);
                }
                System.setProperty(ConfigProperties.PROP_PERSISTENCE_READ_BUFFER_SIZE, String.valueOf(parser.getOptionValue(persisterReadBufferSizeOption, 8192)));
                System.setProperty(ConfigProperties.PROP_PERSISTENCE_PAGE_SIZE, String.valueOf(parser.getOptionValue(persisterPageSizeOption, 4096)));

                System.out.println("");
                System.out.println("Parameters");
                System.out.println("...Driver {");
                System.out.println("......encoding=" + System.getProperty(ConfigProperties.PROP_DRIVER_TEST_ENCODING));
                System.out.println("......count=" + System.getProperty(ConfigProperties.PROP_DRIVER_TEST_COUNT));
                System.out.println("......warmupTime=" + System.getProperty(ConfigProperties.PROP_DRIVER_TEST_WARMUP_TIME));
                System.out.println("......rate=" + System.getProperty(ConfigProperties.PROP_DRIVER_TEST_RATE));
                System.out.println("......noLatencyWrites=" + System.getProperty(ConfigProperties.PROP_DRIVER_LW_NOWRITE));
                System.out.println("......printIntervalStats=" + System.getProperty(ConfigProperties.PROP_DRIVER_LW_PRINT_INTERVAL_STATS));
                System.out.println("......injectorCPUAffinityMask=" + System.getProperty(ConfigProperties.PROP_DRIVER_INJECTOR_CPU_AFFINITY_MASK));
                System.out.println("...}");
                System.out.println("...Containerization {");
                final boolean launchInServer = (Boolean)parser.getOptionValue(serverOption, false);
                System.out.println("......launchInServer=" + launchInServer);
                System.out.println("...}");
                System.out.println("...Engine Event Mux {");
                System.out.println("......muxQueueDepth=" + System.getProperty(ConfigProperties.PROP_MUX_QUEUE_DEPTH));
                System.out.println("......muxCPUAffinityMask=" + System.getProperty(ConfigProperties.PROP_MUX_CPU_AFFINITY_MASK));
                System.out.println("...}");
                System.out.println("...Store Persister {");
                System.out.println("......enabled=" + enablePersistence);
                if (enablePersistence) {
                    System.out.println("......initialLogLength=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_INITIAL_LOG_LENGTH));
                    System.out.println("......zeroOutInitial=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_ZERO_OUT_INITIAL));
                    System.out.println("......flushUsingMappedMemory=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_FLUSH_USING_MAPPED_MEMORY));
                    System.out.println("......writeBufferSize=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_WRITE_BUFFER_SIZE));
                    System.out.println("......flushOnCommit=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_FLUSH_ON_COMMIT));
                    System.out.println("......detached=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_DETACHED));
                    System.out.println(".........queueDepth=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_DETACHED_QUEUE_DEPTH));
                    System.out.println(".........writerCPUAffinityMask=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_DETACHED_QUEUE_DRAINER_CPU_AFFINITY_MASK));
                    System.out.println("......readBufferSize=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_READ_BUFFER_SIZE));
                    System.out.println("......pageSize=" + System.getProperty(ConfigProperties.PROP_PERSISTENCE_PAGE_SIZE));
                }
                System.out.println("...}");
                System.out.println("");

                // enable affinitization
                if (injectorCPUAffinityMask != null || muxCPUAffinityMask != null || (enablePersistence && persisterWriterCPUAffinityMask != null)) {
                    System.setProperty(UtlConstants.THREAD_ENABLECPUAFFINITYMASKS_PROPNAME, "true");
                    if (injectorCPUAffinityMask == null || muxCPUAffinityMask == null || (enablePersistence && persisterWriterCPUAffinityMask == null)) {
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
                    final ESProcessor processor = new ESProcessor();

                    // create the engine
                    final AepMessageSender sender = AepMessageSender.create();
                    final AepEngineDescriptor engineDescriptor = AepEngineDescriptor.load("processor");
                    engineDescriptor.setHAPolicy(AepEngine.HAPolicy.EventSourcing);
                    final AepEngine engine = AepEngine.create(engineDescriptor, 
                                                              null,
                                                              new HashSet<Object>(Arrays.asList(new Object[] {processor})), 
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
                Thread.sleep(Long.MAX_VALUE);
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

