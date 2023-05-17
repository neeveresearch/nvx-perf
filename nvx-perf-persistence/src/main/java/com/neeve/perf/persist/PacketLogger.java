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

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Properties;

import com.neeve.io.IOBuffer;
import com.neeve.ods.IStoreBinding;
import com.neeve.ods.StoreBinding;
import com.neeve.ods.StoreCommitEntry;
import com.neeve.perf.common.LatencyWriter;
import com.neeve.perf.serialization.MessageFactory;
import com.neeve.pkt.PktFactory;
import com.neeve.pkt.PktPacket;
import com.neeve.pkt.log.PktRecoveryLog;
import com.neeve.pkt.types.PktBodyData;
import com.neeve.pkt.types.PktBodyTypesBase;
import com.neeve.quark.QuarkBuffer;
import com.neeve.rog.IRogMessage;
import com.neeve.util.UtlConstants;
import com.neeve.util.UtlThread;

import jargs.gnu.CmdLineParser;

final public class PacketLogger {
    final private class PacketProcessor {
        private int _i;
        private long _start;
        private int _warmupTime;
        private boolean _warmupCompleted;
        private int _postWarmupCount;
        private long _postWarmupStart;

        final void start(final int warmupTime) {
            _i = 0;
            _start = System.nanoTime();
            _warmupTime = warmupTime;
            _warmupCompleted = false;
            _postWarmupCount = 0;
            _postWarmupStart = 0;
        }

        final void handlePacket(final PktPacket packet) {
            if (packet != null) {
                packet.dispose();
            }
            _i++;
            if (_warmupCompleted) {
                _postWarmupCount++;
            }
            final long current = System.nanoTime();
            if (!_warmupCompleted && current - _start > (_warmupTime * 1000000000L)) {
                System.out.println("Warm up complete.");
                _postWarmupStart = current;
                _warmupCompleted = true;
            }
        }

        final int count() {
            return _i;
        }

        final void done() {
            final long current = System.nanoTime();
            final int overallRate = (int)((_postWarmupCount * 1000000000L) / (current - _postWarmupStart));
            System.out.println("Read " + _dfmt.format(_postWarmupCount) + " packets @ " + _dfmt.format(overallRate) + " pkts/sec post warmup.");
        }
    }

    final private PktRecoveryLog _log;
    final private DecimalFormat _dfmt;

    PacketLogger(final String logLocation,
                 final int initialLogLength,
                 final boolean zeroOutInitial,
                 final int pageSize,
                 final int autoFlushSize,
                 final boolean flushUsingMappedMemory) throws Exception {
        // create number formatter
        _dfmt = new DecimalFormat("#,###");

        // create log
        System.out.println("Creating log...");
        long ts = System.nanoTime();
        _log = PktRecoveryLog.create(logLocation, 
                                     "perf.log", 
                                     PktRecoveryLog.FileOpenMode.rw,
                                     initialLogLength * 1024l * 1024l * 1024l,
                                     zeroOutInitial,
                                     flushUsingMappedMemory,
                                     autoFlushSize,
                                     pageSize);
        System.out.println("Created log in " + _dfmt.format(((System.nanoTime() - ts)) / 1000l) + " us");
    }

    final private IRogMessage createMessage() {
        return (IRogMessage)new MessageFactory("rumi.xbuf2").createCar(true);
    }

    final private PktPacket populatePacket(final IRogMessage message, final StoreCommitEntry commitEntry) {
        final PktPacket packet = message.serialize();
        if (commitEntry != null) {
            commitEntry.init(IStoreBinding.Operation.Remove,
                             message.getId(),
                             message.getOfid(),
                             message.getType(),
                             0l,
                             0l,
                             0l,
                             message,
                             packet,
                             message.getContentEncodingType(),
                             true,
                             true);
        }
        return packet;
    }

    final private void write(final int count,
                             final int warmupTime,
                             final int rate,
                             final boolean sync,
                             final boolean supportTailing,
                             final boolean populateStorePacketMetadata,
                             final int flushAfter,
                             final long nanoTimeOverhead,
                             final boolean noLatencyWrites,
                             final boolean printIntervalStats) throws Exception {
        // create and populate the source messsage
        final IRogMessage message = createMessage();

        // create the commit entry used to prepare the store metadata in the packet
        final StoreCommitEntry commitEntry = populateStorePacketMetadata ? StoreCommitEntry.create() : null;

        // create latency writers
        final LatencyWriter prepTimes = new LatencyWriter("prep", noLatencyWrites ? null : "latencies.prep.bin", true, printIntervalStats, false);
        final LatencyWriter writeTimes = new LatencyWriter("write", noLatencyWrites ? null : "latencies.write.bin", false, printIntervalStats, false);
        final LatencyWriter flushTimes = new LatencyWriter("flush", noLatencyWrites ? null : "latencies.flush.bin", false, printIntervalStats, false);
        final LatencyWriter totalTimes = new LatencyWriter("total", noLatencyWrites ? null : "latencies.total.bin", false, printIntervalStats, false);

        System.out.println("Writing...");
        int i = 0;
        final long start = System.nanoTime();
        final long nanosPerMsg = rate > 0 ? (1000000000l / rate) : 0;
        long next = start + nanosPerMsg;
        boolean warmupCompleted = false;
        int postWarmupCount = 0;
        long postWarmupStart = 0;
        long numToFlush = flushAfter <= 0 ? Long.MAX_VALUE : flushAfter;
        int numFlushes = 0;
        prepTimes.start(rate, count);
        writeTimes.start(rate, count);
        flushTimes.start(rate, count);
        totalTimes.start(rate, count);
        while (i < count) {
            final long current = System.nanoTime();
            if (current >= next) {
                // prepare the packet for write
                long t0 = System.nanoTime();
                final PktPacket packet = populatePacket(message, commitEntry);
                long t1 = System.nanoTime();
                final int prepTime = (int)(t1 - t0 - nanoTimeOverhead);

                // write packet
                _log.write(packet, sync ? PktRecoveryLog.FLG_SYNC : 0);
                packet.dispose();
                final int writeTime = (int)(System.nanoTime() - t1 - nanoTimeOverhead);

                // check and flush to log
                int flushTime = -1;
                if (--numToFlush == 0) {
                    t1 = System.nanoTime();
                    _log.flush(sync ? PktRecoveryLog.FLG_SYNC : 0);
                    flushTime = (int)(System.nanoTime() - t1 - nanoTimeOverhead);
                    numFlushes++;
                    numToFlush = flushAfter;
                }

                // record times
                prepTimes.write(prepTime);
                writeTimes.write(writeTime);
                if (flushTime >= 0) {
                    flushTimes.write(flushTime);
                    totalTimes.write(prepTime + flushTime + writeTime);
                }
                else {
                    totalTimes.write(prepTime + writeTime);
                }

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
        prepTimes.stop();
        writeTimes.stop();
        flushTimes.stop();
        totalTimes.stop();
        prepTimes.close(false);
        writeTimes.close(false);
        if (numFlushes > 0) flushTimes.close(false);
        totalTimes.close(false);
        prepTimes.finish();

        // print throughput stats
        final int overallRate = (int)((postWarmupCount * 1000000000L) / (stop - postWarmupStart));
        System.out.println("Wrote " + _dfmt.format(postWarmupCount) + " packets @ " + _dfmt.format(overallRate) + " pkts/sec post warmup.");
        System.out.println("Write complete (run rumi-reporter on latencies.*.bin to calculate latency stats)");
    }

    final private void readUsingIterativeReader(final int count, final int warmupTime) throws IOException {
        long ts = System.nanoTime();
        final PktRecoveryLog.Reader reader = _log.createReader();
        try {
            System.out.println("Created reader in " + _dfmt.format(((System.nanoTime() - ts)) / 1000l) + " us");
            final PacketProcessor processor = new PacketProcessor();
            processor.start(warmupTime);
            PktPacket packet;
            for (int j = 0; j < count ; j++) {
                if ((packet = reader.next()) != null) {
                    processor.handlePacket(packet);
                }
            }
            processor.done();
        }
        finally {
            reader.close(); 
        }
    }

    final private void read(final int count, final int warmupTime) throws Exception {
        System.out.println("Reading (count=" + count + ")..."); 
        readUsingIterativeReader(count, warmupTime);
    }

    final private void run(final int count,
                           final int warmupTime,
                           final int rate,
                           final boolean sync,
                           boolean supportTailing,
                           final boolean tail,
                           final boolean populateStorePacketMetadata,
                           final int flushAfter,
                           final boolean noLatencyWrites,
                           final boolean printIntervalStats) throws Exception {
        // dump run parameters
        if (tail) {
            supportTailing = true;
        }
        System.out.println(""); 
        System.out.println("*** Run Parameters");
        System.out.println("*** ...populateStorePacketMetadata=" + populateStorePacketMetadata);
        System.out.println("*** ...flushAfter=" + flushAfter + " (0=flush disabled)");
        System.out.println("*** ...sync=" + sync);
        System.out.println("***");
        System.out.println("*** ...count=" + count);
        System.out.println("*** ...warmupTime=" + warmupTime);
        System.out.println("*** ...rate=" + rate);
        System.out.println("***");
        System.out.println("*** ...supportTailing=" + supportTailing);
        System.out.println("*** ...tail=" + tail);
        System.out.println("***");
        System.out.println("*** ...noLatencyWrites=" + noLatencyWrites);
        System.out.println("*** ...printIntervalStats=" + printIntervalStats);
        System.out.println("");

        // calculate nanoTime overhead
        long nanoTimeOverhead = 0l;
        long start = System.nanoTime();
        System.out.println("Calculating nanoTime() overhead...");
        for (int i = 0; i < 100000000; i++) {
            System.nanoTime();
        }
        nanoTimeOverhead = (System.nanoTime() - start) / 100000000;
        
        // write and read
        try {
            // open log
            System.out.println("Opening log...");
            long ts = System.nanoTime();
            _log.open(supportTailing ? PktRecoveryLog.FLG_SUPPORT_TAILING : 0);
            System.out.println("Opened log in " + _dfmt.format(((System.nanoTime() - ts)) / 1000l) + " us");

            // if tail, then start the concurrent read
            if (tail) {
                new Thread() {
                    @Override
                    final public void run() {
                        try {
                            read(count, warmupTime);
                        }
                        catch (Throwable e) {
                            e.printStackTrace();
                        }
                    }
                }.start();
            }

            // write
            write(count, 
                  warmupTime, 
                  rate, 
                  sync, 
                  supportTailing, 
                  populateStorePacketMetadata, 
                  flushAfter, 
                  nanoTimeOverhead, 
                  noLatencyWrites,
                  printIntervalStats);
            _log.flush(sync ? PktRecoveryLog.FLG_SYNC : 0);

            // spacer
            System.out.println(""); 

            // read
            if (!tail) {
                // close log
                System.out.println("Closing log...");
                ts = System.nanoTime();
                _log.close();
                System.out.println("Log closed in " + _dfmt.format(((System.nanoTime() - ts)) / 1000l) + " us");

                // open again
                System.out.println("Opening log...");
                ts = System.nanoTime();
                _log.open(supportTailing ? PktRecoveryLog.FLG_SUPPORT_TAILING : 0);
                System.out.println("Opened log in " + _dfmt.format(((System.nanoTime() - ts)) / 1000l) + " us");

                // read
                read(count, warmupTime);
            }
        }
        finally {
            System.out.println("Closing log...");
            _log.close();
        }
    }

    private static void printUsage() {
        System.err.println("Usage PacketLogger");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println("  [{-l, --logLocation} the directory where to create the log]");
        System.err.println("    Specifies the directory where the log should be created (default=\".\")");
        System.err.println("  [{-i, --initialLogLength} the preallocated length of the log]");
        System.err.println("    Specifies the preallocated length (in gigabytes) of the log (default=1)");
        System.err.println("  [{-z, --zeroOutInitial} zeroes out the preallocated length. only applies if --initialLength is specified and > 0]");
        System.err.println("    Specifies whether to zero out the preallocated length of the log (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println("  [{-v, --populateStorePacketMetadata} populate metadata in packets being written as done by ODS binding when storing/replicating packets]");
        System.err.println("    Populate packet metadata in packets being written (default=false)");
        System.err.println("  [{-a, --autoFlushSize} specifies the auto flush size]");
        System.err.println("    Specifies (in bytes) the threshold on in-memory cached log entries that trigger automatic flushes (default=8192)");
        System.err.println("  [{-f, --flushAfter} number of writes to flush after]");
        System.err.println("    Number of writes to flush after (default=0 i.e. no explicit flush)");
        System.err.println("  [{-m, --flushUsingMappedMemory} whether to flush using a memory mapped IO");
        System.err.println("    Specifies whether to use a memory mapped IO to perform flush operations (default=false)");
        System.err.println("  [{-y, --sync} sync during flush]");
        System.err.println("    Sync when flushing (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println("  [{-p, --pageSize} specifies the disk subsystem page size]");
        System.err.println("    Specifies (in bytes) the default for read buffer and write buffer sizes (default=8192)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println("  [{-t, --supportTailing} run in mode that supports tailing]");
        System.err.println("    Run in mode that supports tailing (default=false)");
        System.err.println("  [{-x, --tail} run in tail mode]");
        System.err.println("    Run in tail mode i.e. concurrent read write. This option implicitly switches on --supportTailing (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println("  [{-c, --count} number of packets to write]");
        System.err.println("    Number of packets to write (default=10,000,000)");
        System.err.println("  [{-t, --warmupTime} Warmup time]");
        System.err.println("    Warmup time, in seconds, for calculation of throughput stats (default=2 (2 seconds))");
        System.err.println("  [{-r, --rate} packet write rate]");
        System.err.println("    Rate at which to write packets (default=-1 (unlimited))");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println("  [{-a, --noLatencyWrites} don't write latencies to a file");
        System.err.println("    Indicates that latencies should not be written to a file (default=false)");
        System.err.println("  [{-b, --printIntervalStats} print interval latency stats");
        System.err.println("    Indicates that latencies stats should be printed on a periodic basis in addition to at the end (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println("  [{-j, --affinity} CPU affinity of the write/read thread");
        System.err.println("    Sets the CPU affinity of the thread performing the read/write (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println("  [{-h, --help} print this help string]");
        System.err.println("");
    }

    // entry point
    final public static void main(final String[] args) throws Exception {
        final CmdLineParser parser = new CmdLineParser();

        // log file related options
        final CmdLineParser.Option logLocationOption = parser.addStringOption('l', "logLocation");
        final CmdLineParser.Option initialLogLengthOption = parser.addIntegerOption('i', "initialLogLength");
        final CmdLineParser.Option zeroOutInitialOption = parser.addBooleanOption('z', "zeroOutInitial");

        // write related options
        final CmdLineParser.Option populateStorePacketMetadataOption = parser.addBooleanOption('v', "populateStorePacketMetadata");
        final CmdLineParser.Option autoFlushSizeOption = parser.addIntegerOption('a', "autoFlushSize");
        final CmdLineParser.Option flushAfterOption = parser.addIntegerOption('f', "flushAfter");
        final CmdLineParser.Option flushUsingMappedMemoryOption = parser.addBooleanOption('m', "flushUsingMappedMemory");
        final CmdLineParser.Option syncOption = parser.addBooleanOption('y', "sync");

        // read related options
        final CmdLineParser.Option pageSizeOption = parser.addIntegerOption('p', "pageSize");

        // tail related options
        final CmdLineParser.Option supportTailingOption = parser.addBooleanOption('t', "supportTailing");
        final CmdLineParser.Option tailOption = parser.addBooleanOption('x', "tail");

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

                // run
                new PacketLogger((String)parser.getOptionValue(logLocationOption, "."), 
                                 (Integer)parser.getOptionValue(initialLogLengthOption, 1),
                                 (Boolean)parser.getOptionValue(zeroOutInitialOption, false),
                                 (Integer)parser.getOptionValue(pageSizeOption, 4096),
                                 (Integer)parser.getOptionValue(autoFlushSizeOption, 8192),
                                 (Boolean)parser.getOptionValue(flushUsingMappedMemoryOption, false)).run((Integer)parser.getOptionValue(countOption, 10000000),
                                                                                                          (Integer)parser.getOptionValue(warmupTimeOption, 2),
                                                                                                          (Integer)parser.getOptionValue(rateOption, -1),
                                                                                                          (Boolean)parser.getOptionValue(syncOption, false),
                                                                                                          (Boolean)parser.getOptionValue(supportTailingOption, false),
                                                                                                          (Boolean)parser.getOptionValue(tailOption, false),
                                                                                                          (Boolean)parser.getOptionValue(populateStorePacketMetadataOption, false),
                                                                                                          (Integer)parser.getOptionValue(flushAfterOption, 0),
                                                                                                          (Boolean)parser.getOptionValue(noLatencyWritesOption, false),
                                                                                                          (Boolean)parser.getOptionValue(printIntervalStatsOption, false));
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
