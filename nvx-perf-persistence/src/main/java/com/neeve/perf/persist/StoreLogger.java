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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import com.neeve.io.IOBuffer;
import com.neeve.ods.IStoreBinding;
import com.neeve.ods.IStoreReader;
import com.neeve.ods.StoreBinding;
import com.neeve.ods.StoreCommitEntry;
import com.neeve.ods.StoreObjectFactoryRegistry;
import com.neeve.perf.serialization.CarFactory;
import com.neeve.pkt.PktFactory;
import com.neeve.pkt.PktPacket;
import com.neeve.pkt.types.PktBodyTypesBase;
import com.neeve.rog.IRogMessage;
import com.neeve.rog.log.RogLog;
import com.neeve.rog.log.RogLogReader;
import com.neeve.util.UtlConstants;
import com.neeve.util.UtlThread;

final public class StoreLogger {
    final private RogLog _logger;

    StoreLogger(final Properties props) throws Exception {
        _logger = RogLog.create("perf", props);
    }

    final private void registerFactories() throws Exception {
        StoreObjectFactoryRegistry.getInstance().registerObjectFactory(new com.neeve.perf.serialization.rumi.xbuf2.CarFactory());
    }

    final private void prepareCommitEntry(final StoreCommitEntry commitEntry, final IRogMessage message, final boolean commitEnd) { 
        commitEntry.init(IStoreBinding.Operation.Remove,
                         message.getId(),
                         message.getOfid(),
                         message.getType(),
                         0l,
                         0l,
                         0l,
                         message,
                         message.serialize(),
                         message.getContentEncodingType(),
                         false,
                         commitEnd);
    }

    final private void clearCommitEntry(final StoreCommitEntry commitEntry) {
        commitEntry.serializedObject.dispose();
        commitEntry.fin();
    }

    final private void write(final int count,
                             final int rate,
                             final int numPerCommit,
                             final boolean syncOnCommit,
                             final boolean recordTimes,
                             final long nanoTimeOverhead) {
        System.out.println("Writing...");
        final int[] populateTimes = recordTimes ? new int[count] : null;
        final int[] writeTimes = recordTimes ? new int[count] : null;
        final int[] totalTimes = recordTimes ? new int[count] : null;
        try {
            int i = 1;
            final long start = System.nanoTime();
            long istart = start;
            int di = 0;
            final long nanosPerMsg = rate > 0 ? (1000000000l / rate) : 0;
            long next = start + nanosPerMsg;
            boolean warmUpCompleted = false;
            int postWarmUpCount = 0;
            long postWarmUpStart = 0;
            final StoreCommitEntry commitEntry = StoreCommitEntry.create();
            final IRogMessage message = (IRogMessage)new CarFactory("rumi.xbuf2").createCar(true);
            while (i < count) {
                final long current = System.nanoTime();
                if (current >= next) {
                    final boolean commitStart = numPerCommit == 1 || ((i+1) % numPerCommit) == 1;
                    final boolean commitEnd = numPerCommit == 1 || ((i+1) % numPerCommit) == 0;
                    final long t0 = recordTimes ? System.nanoTime() : 0l;
                    prepareCommitEntry(commitEntry, message, commitEnd);
                    final long t1 = recordTimes ? System.nanoTime() : 0l;
                    if (recordTimes) populateTimes[i] = (int)(t1 - t0 - nanoTimeOverhead);
                    _logger.writeCommitEntry(commitEntry, true, commitEnd && syncOnCommit);
                    clearCommitEntry(commitEntry);
                    if (recordTimes) writeTimes[i] = (int)(System.nanoTime() - t1 - nanoTimeOverhead);
                    if (recordTimes) totalTimes[i] = populateTimes[i] + writeTimes[i];
                    next += nanosPerMsg;
                    di++;
                    i++;
                    if (warmUpCompleted) {
                        postWarmUpCount++;
                    }
                }
                if (!warmUpCompleted && current - start > 5000000000L) { // 5 second warmup
                    System.out.println("Warm up complete.");
                    postWarmUpStart = current;
                    warmUpCompleted = true;
                }
                if (current - istart > 1000000000L) { // every 1 second
                    final int effectiveRate = (int)((di * 1000000000L) / (current - istart));
                    System.out.println("Written=" + i + " Rate=" + effectiveRate);
                    istart = current;
                    di = 0;
                }
            }
            _logger.flush(syncOnCommit);
            final long current = System.nanoTime();
            final int overallRate = (int)((postWarmUpCount * 1000000000L) / (current - postWarmUpStart));
            System.out.println("Done (Written=" + i + " [Post Warmup=" + postWarmUpCount + "] Rate=" + overallRate + ")");
            if (recordTimes) {
                try {
                    // prep times
                    System.out.println("Writing prep times...");
                    FileOutputStream fos = new FileOutputStream(new File("populatetimes.bin"));
                    DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(fos, 8192));
                    for (int k = 0; k < count; k++) {
                        dos.writeInt(k);
                        dos.writeInt(populateTimes[k]);
                    }
                    dos.flush();
                    dos.close();

                    // write times
                    System.out.println("Writing write times...");
                    fos = new FileOutputStream(new File("writetimes.bin"));
                    dos = new DataOutputStream(new BufferedOutputStream(fos, 8192));
                    for (int k = 0; k < count; k++) {
                        dos.writeInt(k);
                        dos.writeInt(writeTimes[k]);
                    }
                    dos.flush();
                    dos.close();

                    // total times
                    System.out.println("Total times...");
                    fos = new FileOutputStream(new File("totaltimes.bin"));
                    dos = new DataOutputStream(new BufferedOutputStream(fos, 8192));
                    for (int k = 0; k < count; k++) {
                        dos.writeInt(k);
                        dos.writeInt(totalTimes[k]);
                    }
                    dos.flush();
                    dos.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("Done");
            }
        }
        catch (Exception e) {
            System.out.println("Write failure [" + e.toString() + "]...");
            e.printStackTrace();
        }
    }

    final private void readUsingLogReader(final int count, final boolean lazyDeserialize) {
        System.out.println("Reading...");
        try {
            long ts = System.nanoTime();
            final RogLogReader reader = _logger.createReader();
            reader.setLazyDeserialization(lazyDeserialize);
            System.out.println("Created reader in " + (((System.nanoTime() - ts)) / 1000l) + " us");

            int i = 0;
            int di = 0;
            final long start = System.nanoTime();
            long istart = start;
            boolean warmUpCompleted = false;
            int postWarmUpCount = 0;
            long postWarmUpStart = 0;
            RogLog.Entry entry;
            for (int j = 0; j < count; j++) {
                if ((entry = reader.next()) != null) {
                    entry.dispose();
                    di++;
                    i++;
                    if (warmUpCompleted) {
                        postWarmUpCount++;
                    }
                    final long current = System.nanoTime();
                    if (!warmUpCompleted && current - start > 5000000000L) { // 5 second warmup
                        System.out.println("Warm up complete.");
                        postWarmUpStart = current;
                        warmUpCompleted = true;
                    }
                    if (current - istart > 1000000000L) { // every 1 second
                        final int effectiveRate = (int)((di * 1000000000L) / (current - istart));
                        System.out.println("Read=" + i + " Rate=" + effectiveRate);
                        istart = current;
                        di = 0;
                    }
                }
            }
            final long current = System.nanoTime();
            final int overallRate = (int)((postWarmUpCount * 1000000000L) / (current - postWarmUpStart));
            System.out.println("Done (Read=" + i + " [Post Warmup=" + postWarmUpCount + "] Rate=" + overallRate + ")");
        }
        catch (Exception e) {
            System.out.println("Failure [" + e.toString() + "]...");
            e.printStackTrace();
        }
    }

    final private void readUsingStoreReader(final int count) {
        System.out.println("Reading...");
        try {
            long ts = System.nanoTime();
            final IStoreReader.IterativeReader reader = _logger.iterativeReader(0);
            System.out.println("Created reader in " + (((System.nanoTime() - ts)) / 1000l) + " us");

            int i = 0;
            int di = 0;
            final long start = System.nanoTime();
            long istart = start;
            boolean warmUpCompleted = false;
            int postWarmUpCount = 0;
            long postWarmUpStart = 0;
            StoreCommitEntry entry;
            for (int j = 0; j < count; j++) {
                if ((entry = reader.next()) != null) {
                    entry.serializedObject.dispose();
                    entry.fin();
                    di++;
                    i++;
                    if (warmUpCompleted) {
                        postWarmUpCount++;
                    }
                    final long current = System.nanoTime();
                    if (!warmUpCompleted && current - start > 5000000000L) { // 5 second warmup
                        System.out.println("Warm up complete.");
                        postWarmUpStart = current;
                        warmUpCompleted = true;
                    }
                    if (current - istart > 1000000000L) { // every 1 second
                        final int effectiveRate = (int)((di * 1000000000L) / (current - istart));
                        System.out.println("Read=" + i + " Rate=" + effectiveRate);
                        istart = current;
                        di = 0;
                    }
                }
            }
            final long current = System.nanoTime();
            final int overallRate = (int)((postWarmUpCount * 1000000000L) / (current - postWarmUpStart));
            System.out.println("Done (Read=" + i + " [Post Warmup=" + postWarmUpCount + "] Rate=" + overallRate + ")");
        }
        catch (Exception e) {
            System.out.println("Failure [" + e.toString() + "]...");
            e.printStackTrace();
        }
    }

    final private void run(final int count,
                           final int rate,
                           final boolean lazyDeserialize,
                           final int numPerCommit,
                           final boolean syncOnCommit,
                           final boolean useStoreReader,
                           final boolean recordTimes) throws Exception {
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

            // open logger
            System.out.println("Opening logger...");
            try {
                _logger.open();
            }
            catch (Exception e) {
                System.out.println("Failed to open the logger [" + e.toString() + "]...");
                e.printStackTrace();
                return;
            }

            // write
            write(count, rate, numPerCommit, syncOnCommit, recordTimes, nanoTimeOverhead);

            // spacer
            System.out.println("");

            // read
            if (useStoreReader) {
                readUsingStoreReader(count);
            }
            else {
                readUsingLogReader(count, lazyDeserialize);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            _logger.close();
        }
    }

    private static void printUsage() {
        System.err.println("Usage StoreLogger");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-o, --logMode} the transaction log open mode]");
        System.err.println("   Specifies the mode to open the transaction log in, Valid values are 'rw', 'rws' and 'rwd' (default='rw')");
        System.err.println(" [{-i, --initialLogLength} the preallocated length of the transaction log]");
        System.err.println("   Specifies the preallocated length (in gigabytes) of the transaction log (default=1)");
        System.err.println(" [{-z, --zeroOutInitial} zeroes out the preallocated length. only applies if --initialLength is specified and > 0]");
        System.err.println("   Specifies whether to zero out the preallocated length of the transaction log (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-a, --writeBufferSize} specifies the log write buffer size]");
        System.err.println("   Specifies, in bytes, the log's write buffer size (default=8192)");
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
        System.err.println(" [{-b, --readBufferSize} specifies the log read buffer size]");
        System.err.println("   Specifies, in bytes, the log's read buffer size (default=8192)");
        System.err.println(" [{-p, --pageSize} specifies the disk subsystem page size]");
        System.err.println("   Specifies (in bytes) the page size to use when reading/writing from/to disk (default=8192)");
        System.err.println(" [{-k, --lazyDeserialize} do not invoke getObject() on read]");
        System.err.println("   A value of false will cause Entry.getObject() to be invoked on read. A value of true will not (default=false)");
        System.err.println(" [{-u, --useStoreReader} use an store reader to read the log file]");
        System.err.println("   A value of true will cause an store reader to be used to read the log. Otherwise, a log reader is used. (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-c, --count} number of messages to persist]");
        System.err.println("   Number of messages to persist (default=10,000,000)");
        System.err.println(" [{-r, --rate} write rate]");
        System.err.println("   Rate at which to persist messages (default=-1 (unlimited))");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-q, --time} record write times");
        System.err.println("   Records how long it takes to write each message to the log (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-j, --affinity} CPU affinity of the write/read thread");
        System.err.println("   Sets the CPU affinity of the thread performing the read/write (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-h, --help} print this help string]");
    }

    // entry point
    final public static void main(final String[] args) throws Exception {
        final CmdLineParser parser = new CmdLineParser();
        final CmdLineParser.Option detachedOption = parser.addBooleanOption('d', "detached");
        final CmdLineParser.Option queueDepthOption = parser.addIntegerOption('q', "queueDepth");
        final CmdLineParser.Option publisherClaimStrategyOption = parser.addStringOption('l', "publisherClaimStrategy");
        final CmdLineParser.Option writerWaitStrategyOption = parser.addStringOption('w', "writerWaitStrategy");
        final CmdLineParser.Option writerAffinityOption = parser.addStringOption('x', "writerAffinity");
        final CmdLineParser.Option logModeOption = parser.addStringOption('m', "logMode");
        final CmdLineParser.Option initialLogLengthOption = parser.addIntegerOption('i', "initialLogLength");
        final CmdLineParser.Option zeroOutInitialOption = parser.addBooleanOption('z', "zeroOutInitial");
        final CmdLineParser.Option pageSizeOption = parser.addIntegerOption('p', "pageSize");
        final CmdLineParser.Option flushUsingMappedMemoryOption = parser.addBooleanOption('m', "flushUsingMappedMemory");
        final CmdLineParser.Option numPerCommitOption = parser.addIntegerOption('n', "numPerCommit");
        final CmdLineParser.Option flushOnCommitOption = parser.addBooleanOption('f', "flushOnCommit");
        final CmdLineParser.Option syncOnCommitOption = parser.addBooleanOption('y', "syncOnCommit");
        final CmdLineParser.Option rateOption = parser.addIntegerOption('r', "rate");
        final CmdLineParser.Option countOption = parser.addIntegerOption('c', "count");
        final CmdLineParser.Option recordTimesOption = parser.addBooleanOption('q', "time");
        final CmdLineParser.Option lazyDeserializeOption = parser.addBooleanOption('k', "lazyDeserialize");
        final CmdLineParser.Option useStoreReaderOption = parser.addBooleanOption('u', "useStoreReader");
        final CmdLineParser.Option affinityOption = parser.addStringOption('j', "affinity");
        final CmdLineParser.Option helpOption = parser.addBooleanOption('h', "help");
        try {
            parser.parse(args);
            if (!((Boolean)parser.getOptionValue(helpOption, false))) {
                final String affinityStr = (String)parser.getOptionValue(affinityOption , null);
                if (affinityStr != null) {
                    System.setProperty(UtlConstants.THREAD_ENABLECPUAFFINITYMASKS_PROPNAME, "true");
                    UtlThread.setCPUAffinityMask(UtlThread.parseAffinityMask(affinityStr));
                }

                final Properties props = new Properties();
                props.setProperty(RogLog.PROP_DETACHED, ((Boolean)parser.getOptionValue(detachedOption, false)) ? "true" : "false");
                props.setProperty(RogLog.PROP_DETACHED_QUEUE_DEPTH, String.valueOf(parser.getOptionValue(queueDepthOption, 1024)));
                props.setProperty(RogLog.PROP_DETACHED_QUEUE_OFFER_STRATEGY, (String)parser.getOptionValue(publisherClaimStrategyOption, "SingleThreaded"));
                props.setProperty(RogLog.PROP_DETACHED_QUEUE_WAIT_STRATEGY, (String)parser.getOptionValue(writerWaitStrategyOption, "Yielding"));
                props.setProperty(RogLog.PROP_DETACHED_QUEUE_DRAINER_CPU_AFFINITIZATION_MASK, (String)parser.getOptionValue(writerAffinityOption, "[0]"));
                props.setProperty(RogLog.PROP_FLUSH_ON_COMMIT, ((Boolean)parser.getOptionValue(flushOnCommitOption, false)) ? "true" : "false");
                props.setProperty(RogLog.PROP_LOG_MODE, (String)parser.getOptionValue(logModeOption, "rw"));
                props.setProperty(RogLog.PROP_INITIAL_LOG_LENGTH, String.valueOf(parser.getOptionValue(initialLogLengthOption, 1)));
                props.setProperty(RogLog.PROP_ZERO_OUT_INITIAL, ((Boolean)parser.getOptionValue(zeroOutInitialOption, false)) ? "true" : "false");
                props.setProperty(RogLog.PROP_PAGE_SIZE, String.valueOf(parser.getOptionValue(pageSizeOption, 4096)));
                props.setProperty(RogLog.PROP_FLUSH_USING_MAPPED_MEMORY, ((Boolean)parser.getOptionValue(flushUsingMappedMemoryOption, false)) ? "true" : "false");

                System.out.println("");
                System.out.println("***** Parameters");
                System.out.println("***** ...detached=" + props.getProperty(RogLog.PROP_DETACHED));
                System.out.println("***** ......queueDepth=" + props.getProperty(RogLog.PROP_DETACHED_QUEUE_DEPTH));
                System.out.println("***** ......publisherClaimStrategy=" + props.getProperty(RogLog.PROP_DETACHED_QUEUE_OFFER_STRATEGY));
                System.out.println("***** ......writerWaitStrategy=" + props.getProperty(RogLog.PROP_DETACHED_QUEUE_WAIT_STRATEGY));
                System.out.println("***** ......writerAffinity=" + props.getProperty(RogLog.PROP_DETACHED_QUEUE_DRAINER_CPU_AFFINITIZATION_MASK));
                System.out.println("***** ...flushOnCommit=" + props.getProperty(RogLog.PROP_FLUSH_ON_COMMIT));
                System.out.println("***** ...logMode=" + props.getProperty(RogLog.PROP_LOG_MODE));
                System.out.println("***** ...initialLogLength=" + props.getProperty(RogLog.PROP_INITIAL_LOG_LENGTH));
                System.out.println("***** ...zeroOutInitial=" + props.getProperty(RogLog.PROP_ZERO_OUT_INITIAL));
                System.out.println("***** ...pageSize=" + props.getProperty(RogLog.PROP_PAGE_SIZE));
                System.out.println("***** ...flushUsingMappedMemory=" + props.getProperty(RogLog.PROP_FLUSH_USING_MAPPED_MEMORY));
                final int count = (Integer)parser.getOptionValue(countOption, 10000000);
                System.out.println("***** ...count=" + count);
                final int rate = (Integer)parser.getOptionValue(rateOption, -1);
                System.out.println("***** ...rate=" + rate);
                final boolean lazyDeserialize = ((Boolean)parser.getOptionValue(lazyDeserializeOption, true));
                System.out.println("***** ...lazyDeserialize=" + lazyDeserialize);
                final boolean useStoreReader = ((Boolean)parser.getOptionValue(useStoreReaderOption, false));
                System.out.println("***** ...useStoreReader=" + useStoreReader);
                final int numPerCommit = (Integer)parser.getOptionValue(numPerCommitOption, 1);
                System.out.println("***** ...numPerCommit=" + numPerCommit);
                final boolean syncOnCommit = ((Boolean)parser.getOptionValue(syncOnCommitOption, false));
                System.out.println("***** ...syncOnCommit=" + syncOnCommit);
                final boolean recordTimes = ((Boolean)parser.getOptionValue(recordTimesOption, false));
                System.out.println("***** ...recordTimes=" + recordTimes);
                System.out.println("");
                new StoreLogger(props).run(count, rate, lazyDeserialize, numPerCommit, syncOnCommit, useStoreReader, recordTimes);
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
