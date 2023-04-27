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
package com.neeve.perf.link.emx;

import java.nio.ByteBuffer;

import com.neeve.emx.EmxNwLnkConnector;
import com.neeve.emx.EmxNwLnk;
import com.neeve.emx.EmxNwLnkReader;
import com.neeve.emx.EmxNwLnkBlockingReader;
import com.neeve.io.IOBuffer;
import com.neeve.perf.common.SystemProperties;
import com.neeve.tools.interactive.commands.AnnotatedCommand;
import com.neeve.util.UtlGovernor;

@AnnotatedCommand.Command(keywords = "NonBlockingPingPongSender", description = "A non blocking (streaming) sender to test ping pong performance using EMX links")
public class NonBlockingPingPongSender extends AnnotatedCommand {
    final private class ReadCallback implements EmxNwLnkReader.Callback {
        private LatencyRecorder latencyRecorder;
        int numReceived;
        int deltaNumReceived;

        ReadCallback() {
        }

        final ReadCallback reset() {
            numReceived = deltaNumReceived = 0;
            return this;
        }

        final void setLatencyRecorder(final LatencyRecorder latencyRecorder) {
            this.latencyRecorder = latencyRecorder;
        }

        @Override
        final public int handleReadData(final EmxNwLnk link, final IOBuffer iobuf, final int length) {
            final long ts = System.nanoTime();
            final int count = length / messageSize; 
            try {
                if (count > 0) {
                    int messageBoundary = 0;
                    for (int i = 0; i < count; i++) {
                        if (latencyRecorder != null) {
                            latencyRecorder.record((int)((ts - iobuf.getLong(messageBoundary)) / (oneWay ? 2 : 1)));
                        }
                        messageBoundary += messageSize;
                    }
                }
                numReceived += count;
                deltaNumReceived += count;
            }
            catch (Throwable e) {
                e.printStackTrace();
            }
            return messageSize * count; 
        }

        @Override
        final public void handleLinkClosure(final EmxNwLnk lnk) {
            System.out.println("Link closed by peer");
        }

        @Override
        final public void handleLinkFailure(final EmxNwLnk lnk, final Throwable cause) {
            cause.printStackTrace();
        }
    }

    @Option(shortForm = 'd', longForm = "descriptor", required = true, description = "The connection descriptor to use e.g. tcp://192.168.1.7:12000&localifaddr=192.168.1.8&localport=12000&tcpnodelay=true")
    private String descriptor;

    @Option(shortForm = 'm', longForm = "messageSize", defaultValue = "256", required = true, description = "The size of the message to ping pong")
    private int messageSize;

    @Option(shortForm = 'c', longForm = "count", defaultValue = "10000000", description = "The number of messages to send")
    private int count;

    @Option(shortForm = 'r', longForm = "rate", defaultValue = "0", description = "The rate at which to send messages")
    private int rate;

    @Option(shortForm = 'w', longForm = "warmUptime", defaultValue = "2", description = "The warm up time, in seconds")
    private int warmupTime;

    @Option(shortForm = 's', longForm = "stats", defaultValue = "false", description = "Whether to output incremental throughput stats")
    private boolean stats;

    @Option(shortForm = 'o', longForm = "oneWayLatency", description = "whether to calculate one-way latency values")
    private boolean oneWay;

    private void stats(final int numSent,
                       final int deltaNumSent,
                       final int numReceived,
                       final int deltaNumReceived,
                       final long now,
                       final long start,
                       final long deltaStart) {
        final long deltaSendRate = deltaStart > 0 ? ((deltaNumSent * 1000l) / (now - deltaStart)) : 0;
        final long overallSendRate = (numSent * 1000l) / (now - start);
        final long deltaReceiveRate = deltaStart > 0 ? ((deltaNumReceived * 1000l) / (now - deltaStart)) : 0;
        final long overallReceiveRate = (numReceived * 1000l) / (now - start);
        final int pending = numSent - numReceived;
        System.out.println("[NonBlockingPingPongSender] SEND [" + numSent + ","  + deltaSendRate + "," + overallSendRate + "]" + " RECV [" + numReceived + ","  + deltaReceiveRate + "," + overallReceiveRate + "]" + " PENDING [" + pending + "]");
    }

    private void waitForRoundTripsToComplete(final int numSent, final ReadCallback cb) {
        System.out.println("[NonBlockingPingPongSender] Waiting for round trips to complete...");
        while (numSent > cb.numReceived) {
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
            }
        }
    }

    public void execute() throws Exception {
        // dump system props
        SystemProperties.dump();

        // establish connection
        System.out.println("[NonBlockingPingPongSender] Establishing link...");
        final EmxNwLnkConnector connector = EmxNwLnkConnector.create(descriptor);
        final EmxNwLnk link = connector.connect();

        // configure the established link
        System.out.println("[NonBlockingPingPongSender] Configuring link (message size=" + messageSize + ")...");
        link.configureBlockingWrite(true);

        // create the latency recorder to hold round trip latencies
        final LatencyRecorder latencyRecorder = new LatencyRecorder(count);

        // start the concurrent reader
        System.out.println("[NonBlockingPingPongSender] Starting the concurrent reader...");
        final ReadCallback cb = new ReadCallback();
        final EmxNwLnkBlockingReader reader = EmxNwLnkBlockingReader.create(link, cb);
        new Thread(reader).start();

        // create the message
        final ByteBuffer[] writeBuffers = new ByteBuffer[] {ByteBuffer.allocateDirect(messageSize)};

        // warm up
        System.out.println("[NonBlockingPingPongSender] Warming up (" + warmupTime + " seconds)...");
        UtlGovernor governor = new UtlGovernor(rate);
        final long warmupStart = System.currentTimeMillis();
        int numSent = 0;
        while ((System.currentTimeMillis() - warmupStart) < (warmupTime * 1000l)) {
            writeBuffers[0].clear(); 
            link.write(writeBuffers, 1); 
            numSent++; 
            governor.blockToNext();
        }
        waitForRoundTripsToComplete(numSent, cb);

        // run test
        System.out.println("[NonBlockingPingPongSender] Running test (rate=" + rate + ", count=" + count + ")...");
        numSent = 0;
        cb.reset().setLatencyRecorder(latencyRecorder);
        governor = new UtlGovernor(rate);
        int deltaNumSent = 0;
        final long start = System.currentTimeMillis();
        long deltaStart = start;
        for (int i = 0 ; i < count ; i++) {
            writeBuffers[0].clear(); 
            writeBuffers[0].putLong(0, System.nanoTime());
            link.write(writeBuffers, 1); 
            numSent++;
            if (stats) {
                deltaNumSent++;
                final long now = System.currentTimeMillis();
                if (now - deltaStart >= 1000l) {
                    stats(numSent, deltaNumSent, cb.numReceived, cb.deltaNumReceived, now, start, deltaStart);
                    deltaStart = now;
                    deltaNumSent = 0;
                    cb.deltaNumReceived = 0;
                }
            }
            governor.blockToNext();
        }
        waitForRoundTripsToComplete(numSent, cb);
        System.out.println("[NonBlockingPingPongSender] Test complete.");
        if (stats) stats(numSent, deltaNumSent, cb.numReceived, cb.deltaNumReceived, System.currentTimeMillis(), start, 0);

        // write stats
        System.out.println("[NonBlockingPingPongSender] Writing stats...");
        latencyRecorder.write("nbpp");
        System.out.println("[NonBlockingPingPongSender] Done (Use rumi-reporter to analyse results)");

        // close link
        reader.stop();
        link.close();
    }

    public static void main(String args[]) throws Exception {
        try {
            NonBlockingPingPongSender sender = new NonBlockingPingPongSender();
            sender.run(args);
        }
        catch (Exception e) {
            System.out.println("[NonBlockingPingPongSender] Received exception during benchmark run - " + e);
        }
    }
}
