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
package com.neeve.perf.ods;

import jargs.gnu.CmdLineParser;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import com.neeve.io.IOBuffer;
import com.neeve.ods.StoreBindingFactory;
import com.neeve.ods.StoreObjectFactoryRegistry;
import com.neeve.perf.common.LatencyWriter;
import com.neeve.perf.serialization.MessageFactory;
import com.neeve.rog.IRogMessage;
import com.neeve.rog.log.RogLog;
import com.neeve.rog.log.RogLogReader;
import com.neeve.util.UtlConstants;
import com.neeve.util.UtlThread;

final public class ESMember extends Common {
    ESMember(final Properties persisterProps) throws Exception {
        super(true, persisterProps, false, null, StoreBindingFactory.FLG_EVENT_SOURCING);
    }

    final private void registerFactories() throws Exception {
        StoreObjectFactoryRegistry.getInstance().registerObjectFactory(new com.neeve.perf.serialization.rumi.xbuf2.MessageFactory());
    }

    final private IRogMessage[] createMessages(final int count) {
        final MessageFactory factory = new MessageFactory("rumi.xbuf2");
        final IRogMessage[] messages = new IRogMessage[count];
        for (int i = 0; i < count ; i++) {
            messages[i] = (IRogMessage)messageFactory.createCar(true);
        }
        return messages;
    }

    final private void run(final int count,
                           final int warmupTime,
                           final int rate,
                           final int numPerCommit,
                           final long nanoTimeOverhead,
                           final boolean noLatencyWrites,
                           final boolean printIntervalStats) throws Exception {
        // create the commit batch
        final IRogMessage[] messages = createMessages(numPerCommit);

        // create latency writer
        final LatencyWriter lw = new LatencyWriter("commit", noLatencyWrites ? null : "latencies.commit.bin", printIntervalStats);

        // run
        System.out.println("Running...");
        int i = 1;
        final long start = System.nanoTime();
        final long nanosPerCommit = rate > 0 ? (1000000000l / rate) : 0;
        long next = start + nanosPerCommit;
        boolean warmupCompleted = false;
        int postWarmupCount = 0;
        long postWarmupStart = 0;
        lw.start(rate, count);
        while (i < count) {
            final long current = System.nanoTime();
            if (current >= next) {
                // commit
                final long t0 = System.nanoTime();
                _store.commit(i, i-1, messages, numPerCommit, null, 0);
                final int commitTime = (int)(System.nanoTime() - t0 - nanoTimeOverhead);

                // record times
                lw.write(commitTime);

                // update counters
                next += nanosPerCommit;
                i++;
                if (warmupCompleted) {
                    postWarmupCount++;
                }
            }
            if (!warmupCompleted && current - start > (warmupTime * 1000000000L)) {
                System.out.println("Warm up complete.");
                postWarmupStart = current;
                warmupCompleted = true;
            }
        }
        final long stop = System.nanoTime();

        // finish latency writing
        lw.close();

        final int overallRate = (int)((postWarmupCount * 1000000000L) / (stop - postWarmupStart));
        System.out.println("Committed " + _dfmt.format(postWarmupCount) + " transactions (" + _dfmt.format(postWarmupCount * numPerCommit) + " messages) @ " + _dfmt.format(overallRate) + " commits/sec (" + _dfmt.format(overallRate * numPerCommit) + " msgs/sec) post warmup.");
    }

    final private void run(final int count, 
                           final int warmupTime,
                           final int rate, 
                           final int numPerCommit,
                           final boolean noLatencyWrites,
                           final boolean printIntervalStats) throws Exception {
        try {
            // calculate nanoTime overhead
            long nanoTimeOverhead = 0l;
            long start = System.nanoTime();
            System.out.println("Calculating nanoTime() overhead...");
            for (int i = 0; i < 100000000; i++) {
                System.nanoTime();
            }
            nanoTimeOverhead = (System.nanoTime() - start) / 100000000;

            // register factories
            System.out.println("Registering factories...");
            registerFactories();

            // run
            run(count, 
                warmupTime, 
                rate, 
                numPerCommit, 
                nanoTimeOverhead, 
                noLatencyWrites, 
                printIntervalStats);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            _store.close(0);
        }
    }

    private static void printUsage() {
        System.err.println("Usage ESMember");
        System.err.println("--------------------------------------------General Parameters------------------------------------------------------");
        System.err.println(" [{-c, --count} number of commits to perform]");
        System.err.println("   Number of commits to perform (default=10,000,000)");
        System.err.println(" [{-t, --warmupTime} Warmup time]");
        System.err.println("   Warmup time, in seconds, for calculation of throughput stats (default=2 (2 seconds))");
        System.err.println(" [{-r, --rate} commit rate]");
        System.err.println("   Rate at which to perform commits (default=-1 (unlimited))");
        System.err.println(" [{-n, --numPerCommit} number of messages per commit]");
        System.err.println("   Number of messages per commit (default=1)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-a, --noLatencyWrites} don't write latencies to a file");
        System.err.println("   Indicates that latencies should not be written to a file (default=false)");
        System.err.println(" [{-b, --printIntervalStats} print interval latency stats");
        System.err.println("   Indicates that latencies stats should be printed on a periodic basis in addition to at the end (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-j, --affinity} CPU affinity of the write/read thread");
        System.err.println("   Sets the CPU affinity of the thread performing the store operations (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-h, --help} print this help string]");
        System.err.println("");
        System.err.println("--------------------------------------------Persister Parameters----------------------------------------------------");
        System.err.println(" [{-o, --logMode} the transaction log open mode]");
        System.err.println("   Specifies the mode to open the transaction log in, Valid values are 'rw', 'rws' and 'rwd' (default='rw')");
        System.err.println(" [{-i, --initialLogLength} the preallocated length of the transaction log]");
        System.err.println("   Specifies the preallocated length (in gigabytes) of the transaction log (default=1)");
        System.err.println(" [{-z, --zeroOutInitial} zeroes out the preallocated length. only applies if --initialLength is specified and > 0]");
        System.err.println("   Specifies whether to zero out the preallocated length of the transaction log (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-m, --flushUsingMappedMemory} whether to flush using a memory mapped region of the log");
        System.err.println("   Specifies whether to use a memory mapped region of the log to perform flush operations (default=false)");
        System.err.println(" [{-f, --flushOnCommit} whether to flush the transaction log on every commit]");
        System.err.println("   Specifies whether the in memory cached entries of the log are forcibly flushed on every commit (default=false)");
        System.err.println("   <Note: A flush does not imply sync>");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-d, --detached} detached]");
        System.err.println("   Switches detached writes (concurrent write in a separate thread) on or off (default=false)");
        System.err.println(" [{-q, --queueDepth} queue depth for detached writes]");
        System.err.println("   Specifies the queue depth for detached writes (default=1024)");
        System.err.println("   <This option only applies to detached writes>");
        System.err.println(" [{-l, --publisherClaimStrategy} disruptor publisher claim strategy for detached writes]");
        System.err.println("   Specifies the disruptor publisher claim strategy. Valid values are SingleThreaded | MultiThreaded | MultiThreadedSufficientCores (default=MultiThreadedSufficientCores)");
        System.err.println("   <This option only applies to detached writes>");
        System.err.println(" [{-w, --writerWaitStrategy} disruptor writer wait strategy for detached writes]");
        System.err.println("   Specifies the disruptor writer claim strategy. Valid values are Sleeping | Yielding | Blocking | BusySpin (default=Yielding)");
        System.err.println("   <This option only applies to detached writes>");
        System.err.println(" [{-x, --writerAffinity} disruptor writer thread affinity for detached writes]");
        System.err.println("   Specifies the disruptor writer thread affinity. (default=[0])");
        System.err.println("   <This option only applies to detached writes>");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-p, --pageSize} specifies the disk subsystem page size]");
        System.err.println("   Specifies (in bytes) the page size to use when reading/writing from/to disk (default=8192)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
    }

    // entry point
    final public static void main(final String[] args) throws Exception {
        final CmdLineParser parser = new CmdLineParser();

        // test parameters
        final CmdLineParser.Option countOption = parser.addIntegerOption('c', "count");
        final CmdLineParser.Option warmupTimeOption = parser.addIntegerOption('t', "warmupTime");
        final CmdLineParser.Option rateOption = parser.addIntegerOption('r', "rate");
        final CmdLineParser.Option numPerCommitOption = parser.addIntegerOption('n', "numPerCommit");

        // latency writer related options
        final CmdLineParser.Option noLatencyWritesOption = parser.addBooleanOption('a', "noLatencyWrites");
        final CmdLineParser.Option printIntervalStatsOption = parser.addBooleanOption('b', "printIntervalStats");

        // affinity related options
        final CmdLineParser.Option affinityOption = parser.addStringOption('j', "affinity");

        // store persister related options
        final CmdLineParser.Option logModeOption = parser.addStringOption('m', "logMode");
        final CmdLineParser.Option initialLogLengthOption = parser.addIntegerOption('i', "initialLogLength");
        final CmdLineParser.Option zeroOutInitialOption = parser.addBooleanOption('z', "zeroOutInitial");
        final CmdLineParser.Option flushUsingMappedMemoryOption = parser.addBooleanOption('m', "flushUsingMappedMemory");
        final CmdLineParser.Option flushOnCommitOption = parser.addBooleanOption('f', "flushOnCommit");
        final CmdLineParser.Option detachedOption = parser.addBooleanOption('d', "detached");
        final CmdLineParser.Option queueDepthOption = parser.addIntegerOption('q', "queueDepth");
        final CmdLineParser.Option publisherClaimStrategyOption = parser.addStringOption('l', "publisherClaimStrategy");
        final CmdLineParser.Option writerWaitStrategyOption = parser.addStringOption('w', "writerWaitStrategy");
        final CmdLineParser.Option writerAffinityOption = parser.addStringOption('x', "writerAffinity");
        final CmdLineParser.Option pageSizeOption = parser.addIntegerOption('p', "pageSize");

        // help 
        final CmdLineParser.Option helpOption = parser.addBooleanOption('h', "help");

        try {
            parser.parse(args);
            if (!((Boolean)parser.getOptionValue(helpOption, false))) {
                // affinitize committer thread
                final String affinityStr = (String)parser.getOptionValue(affinityOption , null);
                if (affinityStr != null) {
                    System.setProperty(UtlConstants.THREAD_ENABLECPUAFFINITYMASKS_PROPNAME, "true");
                    UtlThread.setCPUAffinityMask(UtlThread.parseAffinityMask(affinityStr));
                }

                // store persister props
                final Properties persisterProps = new Properties();
                persisterProps.setProperty(RogLog.PROP_LOG_MODE, (String)parser.getOptionValue(logModeOption, "rw"));
                persisterProps.setProperty(RogLog.PROP_INITIAL_LOG_LENGTH, String.valueOf(parser.getOptionValue(initialLogLengthOption, 1)));
                persisterProps.setProperty(RogLog.PROP_ZERO_OUT_INITIAL, ((Boolean)parser.getOptionValue(zeroOutInitialOption, false)) ? "true" : "false");
                persisterProps.setProperty(RogLog.PROP_FLUSH_USING_MAPPED_MEMORY, ((Boolean)parser.getOptionValue(flushUsingMappedMemoryOption, false)) ? "true" : "false");
                persisterProps.setProperty(RogLog.PROP_FLUSH_ON_COMMIT, ((Boolean)parser.getOptionValue(flushOnCommitOption, false)) ? "true" : "false");
                persisterProps.setProperty(RogLog.PROP_DETACHED, ((Boolean)parser.getOptionValue(detachedOption, false)) ? "true" : "false");
                persisterProps.setProperty(RogLog.PROP_DETACHED_QUEUE_DEPTH, String.valueOf(parser.getOptionValue(queueDepthOption, 1024)));
                persisterProps.setProperty(RogLog.PROP_DETACHED_QUEUE_OFFER_STRATEGY, (String)parser.getOptionValue(publisherClaimStrategyOption, "SingleThreaded"));
                persisterProps.setProperty(RogLog.PROP_DETACHED_QUEUE_WAIT_STRATEGY, (String)parser.getOptionValue(writerWaitStrategyOption, "Yielding"));
                persisterProps.setProperty(RogLog.PROP_DETACHED_QUEUE_DRAINER_CPU_AFFINITIZATION_MASK, (String)parser.getOptionValue(writerAffinityOption, "[0]"));
                persisterProps.setProperty(RogLog.PROP_PAGE_SIZE, String.valueOf(parser.getOptionValue(pageSizeOption, 4096)));

                System.out.println("");
                System.out.println("***** Parameters");
                System.out.println("***** ...Runner ");
                final int count = (Integer)parser.getOptionValue(countOption, 10000000);
                System.out.println("***** ......count=" + count);
                final int warmupTime = (Integer)parser.getOptionValue(warmupTimeOption, 2);
                System.out.println("***** ......warmupTime=" + warmupTime);
                final int rate = (Integer)parser.getOptionValue(rateOption, -1);
                System.out.println("***** ......rate=" + rate);
                final int numPerCommit = (Integer)parser.getOptionValue(numPerCommitOption, 1);
                System.out.println("***** ......numPerCommit=" + numPerCommit);
                final boolean noLatencyWrites = ((Boolean)parser.getOptionValue(noLatencyWritesOption, false));
                System.out.println("***** ......noLatencyWrites=" + noLatencyWrites);
                final boolean printIntervalStats = ((Boolean)parser.getOptionValue(printIntervalStatsOption, false));
                System.out.println("***** ......printIntervalStats=" + printIntervalStats);
                System.out.println("***** ......affinity=" + affinityStr);
                System.out.println("*****");
                System.out.println("***** ...}");
                System.out.println("***** ...Store Persister {");
                System.out.println("***** ......logMode=" + persisterProps.getProperty(RogLog.PROP_LOG_MODE));
                System.out.println("***** ......initialLogLength=" + persisterProps.getProperty(RogLog.PROP_INITIAL_LOG_LENGTH));
                System.out.println("***** ......zeroOutInitial=" + persisterProps.getProperty(RogLog.PROP_ZERO_OUT_INITIAL));
                System.out.println("***** ......flushUsingMappedMemory=" + persisterProps.getProperty(RogLog.PROP_FLUSH_USING_MAPPED_MEMORY));
                System.out.println("***** ......flushOnCommit=" + persisterProps.getProperty(RogLog.PROP_FLUSH_ON_COMMIT));
                System.out.println("***** ......detached=" + persisterProps.getProperty(RogLog.PROP_DETACHED));
                System.out.println("***** .........queueDepth=" + persisterProps.getProperty(RogLog.PROP_DETACHED_QUEUE_DEPTH));
                System.out.println("***** .........publisherClaimStrategy=" + persisterProps.getProperty(RogLog.PROP_DETACHED_QUEUE_OFFER_STRATEGY));
                System.out.println("***** .........writerWaitStrategy=" + persisterProps.getProperty(RogLog.PROP_DETACHED_QUEUE_WAIT_STRATEGY));
                System.out.println("***** .........writerAffinity=" + persisterProps.getProperty(RogLog.PROP_DETACHED_QUEUE_DRAINER_CPU_AFFINITIZATION_MASK));
                System.out.println("***** ......pageSize=" + persisterProps.getProperty(RogLog.PROP_PAGE_SIZE));
                System.out.println("***** ...}");
                System.out.println("");
                new ESMember(persisterProps).run(count, 
                                                 warmupTime, 
                                                 rate, 
                                                 numPerCommit, 
                                                 noLatencyWrites,
                                                 printIntervalStats);
            }
            else {
                printUsage();
            }
        }
        catch (CmdLineParser.OptionException e) {
            System.err.println(e.getMessage());
            printUsage();
        }
    }
}
