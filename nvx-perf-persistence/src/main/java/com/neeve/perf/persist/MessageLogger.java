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
package com.neeve.perf.persist;

import jargs.gnu.CmdLineParser;

import java.text.DecimalFormat;
import java.util.Properties;

import com.neeve.io.IOBuffer;
import com.neeve.ods.IStoreBinding;
import com.neeve.ods.IStoreReader;
import com.neeve.ods.StoreBinding;
import com.neeve.ods.StoreCommitEntry;
import com.neeve.ods.StoreObjectFactoryRegistry;
import com.neeve.perf.common.LatencyWriter;
import com.neeve.perf.serialization.MessageFactory;
import com.neeve.pkt.PktFactory;
import com.neeve.pkt.PktPacket;
import com.neeve.pkt.types.PktBodyTypesBase;
import com.neeve.rog.IRogMessage;
import com.neeve.rog.IRogMessageLogger;
import com.neeve.rog.log.RogLog;
import com.neeve.rog.log.RogLogReader;
import com.neeve.util.UtlConstants;
import com.neeve.util.UtlThread;

final public class MessageLogger {
    final private IRogMessageLogger _logger;
    final private DecimalFormat _dfmt;

    MessageLogger(final Properties props) throws Exception {
        _dfmt = new DecimalFormat("#,###");
        _logger = RogLog.create("perf", props);
    }

    final private IRogMessage createMessage() {
        return (IRogMessage)new MessageFactory("rumi.xbuf2").createCar(true);
    }

    final private void write(final int count,
                             final int warmupTime,
                             final int rate,
                             final int numPerCommit,
                             final boolean syncOnCommit,
                             final long nanoTimeOverhead,
                             final boolean noLatencyWrites,
                             final boolean printIntervalStats) throws Exception {
        // create and populate the source messsage
        final IRogMessage message = (IRogMessage)new MessageFactory("rumi.xbuf2").createCar(true);

        // create latency writer
        final LatencyWriter lw = new LatencyWriter("write", noLatencyWrites ? null : "latencies.write.bin", printIntervalStats);

        // write
        System.out.println("Writing...");
        int i = 1;
        final long start = System.nanoTime();
        final long nanosPerMsg = rate > 0 ? (1000000000l / rate) : 0;
        long next = start + nanosPerMsg;
        boolean warmupCompleted = false;
        int postWarmupCount = 0;
        long postWarmupStart = 0;
        lw.start(rate, count);
        while (i < count) {
            final long current = System.nanoTime();
            if (current >= next) {
                // calc commit start and/or end
                final boolean commitStart = numPerCommit == 1 || ((i+1) % numPerCommit) == 1;
                final boolean commitEnd = numPerCommit == 1 || ((i+1) % numPerCommit) == 0;

                // write 
                final long t0 = System.nanoTime();
                _logger.log(message, commitEnd);
                if (commitEnd && syncOnCommit) {
                    _logger.flush(true);
                }
                final int writeTime = (int)(System.nanoTime() - t0 - nanoTimeOverhead);

                // record times
                lw.write(writeTime);

                // update counters
                next += nanosPerMsg;
                i++;
                if (warmupCompleted) {
                    postWarmupCount++;
                }
            }
            if (!warmupCompleted && current - start > (warmupTime * 1000000000L)) {
                System.out.println("Warm up complete.");
                postWarmupStart = System.nanoTime();
                warmupCompleted = true;
            }
        }
        final long stop = System.nanoTime();

        // finish latency writing
        lw.close();

        // throughput stats
        final int overallRate = (int)((postWarmupCount * 1000000000L) / (stop - postWarmupStart));
        System.out.println("Wrote " + _dfmt.format(postWarmupCount) + " messages @ " + _dfmt.format(overallRate) + " msgs/sec post warmup.");
        System.out.println("Write complete (run rumi-reporter on latencies.write.bin to calculate latency stats)");
    }

    final private void run(final int count,
                           final int warmupTime,
                           final int rate,
                           final int numPerCommit,
                           final boolean syncOnCommit,
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

            // open logger
            System.out.println("Opening logger...");
            _logger.open();

            // write
            write(count, 
                  warmupTime, 
                  rate, 
                  numPerCommit, 
                  syncOnCommit, 
                  nanoTimeOverhead, 
                  noLatencyWrites, 
                  printIntervalStats);

            // flush
            _logger.flush(syncOnCommit);

            // Message logger does not have a native reader
            System.out.println("Message logger does not support a reader. Refer to StoreLogger for message log read performance");

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            System.out.println("Closing logger...");
            _logger.close();
        }
    }

    private static void printUsage() {
        System.err.println("Usage MessageLogger");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-k, --logLocation} the directory where to create the log]");
        System.err.println("   Specifies the directory where the log should be created (default=\".\")");
        System.err.println(" [{-o, --logMode} the transaction log open mode]");
        System.err.println("   Specifies the mode to open the transaction log in, Valid values are 'rw', 'rws' and 'rwd' (default='rw')");
        System.err.println(" [{-i, --initialLogLength} the preallocated length of the transaction log]");
        System.err.println("   Specifies the preallocated length (in gigabytes) of the transaction log (default=1)");
        System.err.println(" [{-z, --zeroOutInitial} zeroes out the preallocated length. only applies if --initialLength is specified and > 0]");
        System.err.println("   Specifies whether to zero out the preallocated length of the transaction log (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-n, --numPerCommit} number of writes per commit]");
        System.err.println("   Number of writes per commit (default=1)");
        System.err.println(" [{-m, --flushUsingMappedMemory} whether to flush using a memory mapped region of the log");
        System.err.println("   Specifies whether to use a memory mapped region of the log to perform flush operations (default=false)");
        System.err.println(" [{-f, --flushOnCommit} whether to flush the transaction log on every commit]");
        System.err.println("   Specifies whether the in memory cached entries of the log are forcibly flushed on every commit (default=false)");
        System.err.println("   <Note: A flush does not imply sync>");
        System.err.println(" [{-y, --syncOnCommit} force sync on every commit]");
        System.err.println("   Forcibly sync to disk on each commit (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-d, --detached} detached]");
        System.err.println("   Switches on detached writes (concurrent write in a separate thread) on or off (default=false)");
        System.err.println(" [{-e, --detachedMessageSerialization} detached message serialization]");
        System.err.println("   Switches on serialization of message to be performed in the detached thread (default=false)");
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
        System.err.println(" [{-c, --count} number of messages to persist]");
        System.err.println("   Number of messages to persist (default=10,000,000)");
        System.err.println(" [{-t, --warmupTime} Warmup time]");
        System.err.println("   Warmup time, in seconds, for calculation of throughput stats (default=2 (2 seconds))");
        System.err.println(" [{-r, --rate} write rate]");
        System.err.println("   Rate at which to persist messages (default=-1 (unlimited))");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-a, --noLatencyWrites} don't write latencies to a file");
        System.err.println("   Indicates that latencies should not be written to a file (default=false)");
        System.err.println(" [{-b, --printIntervalStats} print interval latency stats");
        System.err.println("   Indicates that latencies stats should be printed on a periodic basis in addition to at the end (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-j, --affinity} CPU affinity of the write/read thread");
        System.err.println("   Sets the CPU affinity of the thread performing the read/write (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-h, --help} print this help string]");
    }

    // entry point
    final public static void main(final String[] args) throws Exception {
        final CmdLineParser parser = new CmdLineParser();

        // log file related options
        final CmdLineParser.Option logLocationOption = parser.addStringOption('k', "logLocation");
        final CmdLineParser.Option logModeOption = parser.addStringOption('o', "logMode");
        final CmdLineParser.Option initialLogLengthOption = parser.addIntegerOption('i', "initialLogLength");
        final CmdLineParser.Option zeroOutInitialOption = parser.addBooleanOption('z', "zeroOutInitial");

        // write related options
        final CmdLineParser.Option numPerCommitOption = parser.addIntegerOption('n', "numPerCommit");
        final CmdLineParser.Option flushUsingMappedMemoryOption = parser.addBooleanOption('m', "flushUsingMappedMemory");
        final CmdLineParser.Option flushOnCommitOption = parser.addBooleanOption('f', "flushOnCommit");
        final CmdLineParser.Option syncOnCommitOption = parser.addBooleanOption('y', "syncOnCommit");

        // detached write related options
        final CmdLineParser.Option detachedOption = parser.addBooleanOption('d', "detached");
        final CmdLineParser.Option detachedMessageSerializationOption = parser.addBooleanOption('e', "detachedMessageSerialization");
        final CmdLineParser.Option queueDepthOption = parser.addIntegerOption('q', "queueDepth");
        final CmdLineParser.Option publisherClaimStrategyOption = parser.addStringOption('l', "publisherClaimStrategy");
        final CmdLineParser.Option writerWaitStrategyOption = parser.addStringOption('w', "writerWaitStrategy");
        final CmdLineParser.Option writerAffinityOption = parser.addStringOption('x', "writerAffinity");

        // test parameters
        final CmdLineParser.Option countOption = parser.addIntegerOption('c', "count");
        final CmdLineParser.Option warmupTimeOption = parser.addIntegerOption('t', "warmupTime");
        final CmdLineParser.Option rateOption = parser.addIntegerOption('r', "rate");

        // latency writer related options
        final CmdLineParser.Option noLatencyWritesOption = parser.addBooleanOption('a', "noLatencyWrites");
        final CmdLineParser.Option printIntervalStatsOption = parser.addBooleanOption('b', "printIntervalStats");

        // affinity related options
        final CmdLineParser.Option affinityOption = parser.addStringOption('j', "affinity");
        
        // help
        final CmdLineParser.Option helpOption = parser.addBooleanOption('h', "help");
        try {
            parser.parse(args);
            if (!((Boolean)parser.getOptionValue(helpOption, false))) {
                // affinitize
                final String affinityStr = (String)parser.getOptionValue(affinityOption , null);
                if (affinityStr != null) {
                    System.setProperty(UtlConstants.THREAD_ENABLECPUAFFINITYMASKS_PROPNAME, "true");
                    UtlThread.setCPUAffinityMask(UtlThread.parseAffinityMask(affinityStr));
                }

                // prepare logger properties
                final Properties props = new Properties();
                props.setProperty(RogLog.PROP_STORE_ROOT, (String)parser.getOptionValue(logLocationOption, "."));
                props.setProperty(RogLog.PROP_LOG_MODE, (String)parser.getOptionValue(logModeOption, "rw"));
                props.setProperty(RogLog.PROP_INITIAL_LOG_LENGTH, String.valueOf(parser.getOptionValue(initialLogLengthOption, 1)));
                props.setProperty(RogLog.PROP_ZERO_OUT_INITIAL, ((Boolean)parser.getOptionValue(zeroOutInitialOption, false)) ? "true" : "false");
                props.setProperty(RogLog.PROP_FLUSH_USING_MAPPED_MEMORY, ((Boolean)parser.getOptionValue(flushUsingMappedMemoryOption, false)) ? "true" : "false");
                props.setProperty(RogLog.PROP_FLUSH_ON_COMMIT, ((Boolean)parser.getOptionValue(flushOnCommitOption, false)) ? "true" : "false");
                props.setProperty(RogLog.PROP_DETACHED, ((Boolean)parser.getOptionValue(detachedOption, false)) ? "true" : "false");
                props.setProperty(RogLog.PROP_DETACHED_MESSAGE_SERIALIZATION, ((Boolean)parser.getOptionValue(detachedMessageSerializationOption, false)) ? "true" : "false");
                props.setProperty(RogLog.PROP_DETACHED_QUEUE_DEPTH, String.valueOf(parser.getOptionValue(queueDepthOption, 1024)));
                props.setProperty(RogLog.PROP_DETACHED_QUEUE_OFFER_STRATEGY, (String)parser.getOptionValue(publisherClaimStrategyOption, "SingleThreaded"));
                props.setProperty(RogLog.PROP_DETACHED_QUEUE_WAIT_STRATEGY, (String)parser.getOptionValue(writerWaitStrategyOption, "Yielding"));
                props.setProperty(RogLog.PROP_DETACHED_QUEUE_DRAINER_CPU_AFFINITIZATION_MASK, (String)parser.getOptionValue(writerAffinityOption, "[0]"));

                // dump parameters
                System.out.println("");
                System.out.println("***** Parameters");
                System.out.println("***** ...logMode=" + props.getProperty(RogLog.PROP_LOG_MODE));
                System.out.println("***** ...initialLogLength=" + props.getProperty(RogLog.PROP_INITIAL_LOG_LENGTH));
                System.out.println("***** ...zeroOutInitial=" + props.getProperty(RogLog.PROP_ZERO_OUT_INITIAL));
                System.out.println("*****");
                final int numPerCommit = (Integer)parser.getOptionValue(numPerCommitOption, 1);
                System.out.println("***** ...numPerCommit=" + numPerCommit);
                System.out.println("***** ...flushUsingMappedMemory=" + props.getProperty(RogLog.PROP_FLUSH_USING_MAPPED_MEMORY));
                System.out.println("***** ...flushOnCommit=" + props.getProperty(RogLog.PROP_FLUSH_ON_COMMIT));
                final boolean syncOnCommit = ((Boolean)parser.getOptionValue(syncOnCommitOption, false));
                System.out.println("***** ...syncOnCommit=" + syncOnCommit);
                System.out.println("*****");
                System.out.println("***** ...detached=" + props.getProperty(RogLog.PROP_DETACHED));
                System.out.println("***** ......detachedSerialization=" + props.getProperty(RogLog.PROP_DETACHED_MESSAGE_SERIALIZATION));
                System.out.println("***** ......queueDepth=" + props.getProperty(RogLog.PROP_DETACHED_QUEUE_DEPTH));
                System.out.println("***** ......publisherClaimStrategy=" + props.getProperty(RogLog.PROP_DETACHED_QUEUE_OFFER_STRATEGY));
                System.out.println("***** ......writerWaitStrategy=" + props.getProperty(RogLog.PROP_DETACHED_QUEUE_WAIT_STRATEGY));
                System.out.println("***** ......writerAffinity=" + props.getProperty(RogLog.PROP_DETACHED_QUEUE_DRAINER_CPU_AFFINITIZATION_MASK));
                System.out.println("*****");
                final int count = (Integer)parser.getOptionValue(countOption, 15000000);
                System.out.println("***** ...count=" + count);
                final int warmupTime = (Integer)parser.getOptionValue(warmupTimeOption, 2);
                System.out.println("***** ...warmupTime=" + warmupTime);
                final int rate = (Integer)parser.getOptionValue(rateOption, 500000);
                System.out.println("***** ...rate=" + rate);
                System.out.println("*****");
                final boolean noLatencyWrites = ((Boolean)parser.getOptionValue(noLatencyWritesOption, false));
                System.out.println("***** ...noLatencyWrites=" + noLatencyWrites);
                final boolean printIntervalStats = ((Boolean)parser.getOptionValue(printIntervalStatsOption, false));
                System.out.println("***** ...printIntervalStats=" + printIntervalStats);
                System.out.println("*****");
                System.out.println("***** ...affinity=" + affinityStr);
                System.out.println("");
                new MessageLogger(props).run(count,
                                             warmupTime, 
                                             rate, 
                                             numPerCommit, 
                                             syncOnCommit, 
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
