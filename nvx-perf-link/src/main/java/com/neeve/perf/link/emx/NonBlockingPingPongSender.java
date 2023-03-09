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

import com.neeve.io.IOBuffer;
import com.neeve.emx.EmxNwLnkConnector;
import com.neeve.emx.EmxNwLnk;
import com.neeve.emx.EmxNwLnkReader;
import com.neeve.emx.EmxNwLnkBlockingReader;
import com.neeve.stats.Stats.LatencyManager;
import com.neeve.tools.interactive.commands.AnnotatedCommand;
import com.neeve.perf.common.SystemProperties;
import com.neeve.util.UtlGovernor;

@AnnotatedCommand.Command(keywords = "NonBlockingPingPongSender", description = "A non blocking (streaming) sender to test ping pong performance using EMX links")
public class NonBlockingPingPongSender extends AnnotatedCommand {
    final private class Latencies {
        final private LatencyManager latencyManager = new LatencyManager("pp", 1000000);
        final private long times[] = new long[10240];
        private long tail;
        private long head;
        private int complete;

        final void onSend(final long ts) {
            final int idx = (int)(head++ % times.length);
            times[idx] = ts;
            if ((head - tail) > times.length) {
                System.out.println("*** PROBLEM ***");
            }
        }

        final void onReceive(final long ts) {
            final int idx = (int)(tail++ % times.length);
            latencyManager.add(ts - times[idx]);
        }

        final void reset() {
            tail = head = 0;
        }

        final StringBuilder dump(final StringBuilder sb) {
            latencyManager.compute();
            sb.setLength(0);
            latencyManager.get(sb);
            return sb;
        }
    }

    final private class ReadCallback implements EmxNwLnkReader.Callback {
        final private Latencies latencies;

        ReadCallback(final Latencies latencies) {
            this.latencies = latencies;
        }

        @Override
        final public int handleReadData(final EmxNwLnk link, final IOBuffer iobuf, final int length) {
            final long ts = System.nanoTime();
            final int count = length / serializedMessageSize; 
            try {
                if (count > 0) {
                    for (int i = 0; i < count; i++) {
                        latencies.onReceive(ts);
                    }
                }
            }
            catch (Throwable e) {
                e.printStackTrace();
            }
            return serializedMessageSize * count; 
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
    String descriptor;

    @Option(shortForm = 's', longForm = "serializedMessageSize", defaultValue = "256", required = true, description = "The size of serialized message")
    int serializedMessageSize;

    @Option(shortForm = 'c', longForm = "count", defaultValue = "10000000", description = "The number of messages to send")
    int count;

    @Option(shortForm = 'r', longForm = "rate", defaultValue = "0", description = "The rate at which to send messages")
    int rate;

    public void execute() throws Exception {
        final Latencies latencies = new Latencies();
        final StringBuilder sb = new StringBuilder();
        SystemProperties.dump();
        final ByteBuffer[] writeBuffers = new ByteBuffer[] {ByteBuffer.allocateDirect(serializedMessageSize)};
        final EmxNwLnkConnector connector = EmxNwLnkConnector.create(descriptor);
        final EmxNwLnk link = connector.connect();
        new Thread(EmxNwLnkBlockingReader.create(link, new ReadCallback(latencies))).start();
        link.configureBlockingWrite(true);
        final long start = System.currentTimeMillis();
        UtlGovernor.run(count, rate, new Runnable() {
            private int numSent = 0;
            private int deltaNumSent = 0;
            private long deltaStart = start;
            @Override
            final public void run() {
                try {
                    latencies.onSend(System.nanoTime());
                    writeBuffers[0].clear(); 
                    link.write(writeBuffers, 1); 
                    numSent++; 
                    deltaNumSent++;
                    final long now = System.currentTimeMillis();
                    if (now - deltaStart >= 1000l) {
                        final long deltaRate = (deltaNumSent * 1000l) / (now - deltaStart);
                        final long overallRate = (numSent * 1000l) / (now - start);
                        System.out.println("RATE [" + deltaRate + "," + overallRate + "]");
                        System.out.print(latencies.dump(sb).toString());
                        deltaStart = now;
                        deltaNumSent = 0;
                    }
                }
                catch (Throwable e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });
        link.close();
    }

    public static void main(String args[]) throws Exception {
        try {
            NonBlockingPingPongSender sender = new NonBlockingPingPongSender();
            sender.run(args);
        }
        catch (Exception e) {
            System.out.println("Received exception during benchmark run - " + e);
        }
    }
}
