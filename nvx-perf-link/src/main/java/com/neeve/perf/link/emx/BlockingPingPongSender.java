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
import java.text.DecimalFormat;

import com.neeve.io.IOBuffer;
import com.neeve.emx.EmxNwLnkConnector;
import com.neeve.emx.EmxNwLnk;
import com.neeve.perf.common.SystemProperties;
import com.neeve.stats.Stats.LatencyManager;
import com.neeve.tools.interactive.commands.AnnotatedCommand;
import com.neeve.util.UtlThread;

@AnnotatedCommand.Command(keywords = "BlockingPingPongSender", description = "A blocking sender to test ping pong performance using EMX links")
public class BlockingPingPongSender extends AnnotatedCommand {
    @Option(shortForm = 'd', longForm = "descriptor", required = true, description = "The connection descriptor to use e.g. tcp://192.168.1.7:12000&localifaddr=192.168.1.8&localport=12000&tcpnodelay=true")
    private String _descriptor;

    @Option(shortForm = 'm', longForm = "messageSize", defaultValue = "256", required = true, description = "The size of the message to ping pong")
    private int _messageSize;

    @Option(shortForm = 'c', longForm = "count", defaultValue = "10000000", description = "The number of messages to send")
    private int _testCount;

    @Option(shortForm = 'r', longForm = "rate", defaultValue = "10000", description = "The rate at which to send messages")
    private int _testRate;

    @Option(shortForm = 'a', longForm = "cpuAffinityMask", description = "which CPU(s) to affinitize the sending thread to")
    private String _cpuAffinityMask;

    @Option(shortForm = 'o', longForm = "oneWayLatency", description = "whether to calculate one-way latency values")
    private boolean _oneWay;

    private void doPingPong(final EmxNwLnk link, final ByteBuffer[] writeBuffers, final ByteBuffer readBuffer) throws Exception {
        writeBuffers[0].clear(); 
        link.write(writeBuffers, 1);
        readBuffer.clear();
        do {
            link.read();
        }
        while (readBuffer.position() < _messageSize);
        return;
    }

    public void execute() throws Exception {
        // affinitize to CPU
        if (_cpuAffinityMask != null) {
            System.out.println("[BlockingPingPongSender] Affinitizing thread to CPU " + _cpuAffinityMask);
            UtlThread.setCPUAffinityMask(UtlThread.parseAffinityMask(_cpuAffinityMask));
        }

        // dump system props
        SystemProperties.dump();

        // dump test parameters
        DecimalFormat dfmt = new DecimalFormat("#,###");
        System.out.println("[RdmaStreamingSender] Message size:" + _messageSize);
        System.out.println("[RdmaStreamingSender] Test count:" + dfmt.format(_testCount));
        System.out.println("[RdmaStreamingSender] Test rate:" + dfmt.format(_testRate));
        System.out.println("[RdmaStreamingSender] CPU affinity mask:" + _cpuAffinityMask);
        System.out.println("[RdmaStreamingSender] One way latency:" + _oneWay);

        // establish connection
        System.out.println("[BlockingPingPongSender] Establishing link...");
        final EmxNwLnkConnector connector = EmxNwLnkConnector.create(_descriptor);
        final EmxNwLnk link = connector.connect();

        // configure the established link
        System.out.println("[BlockingPingPongSender] Configuring link (message size=" + _messageSize + ")...");
        final ByteBuffer readBuffer = ByteBuffer.allocateDirect(_messageSize);
        link.setReadBuffer(IOBuffer.wrap(readBuffer));
        link.configureBlockingRead(true);
        link.configureBlockingWrite(true);

        // create the message
        final ByteBuffer[] writeBuffers = new ByteBuffer[] {ByteBuffer.allocateDirect(_messageSize)};

        // calculate nanoTime overhead
        System.out.println("[BlockingPingPongSender] Calculating UtlTime.now() overhead...");
        long nanoTimeOverhead = 0l;
        long start = System.nanoTime();
        for (int i = 0; i < 100000000l; i++) {
            System.nanoTime();
        }
        nanoTimeOverhead = (System.nanoTime() - start) / 100000000l;
        System.out.println("[BlockingPingPongSender] ..." + nanoTimeOverhead + "ns");

        // create latency manager
        final LatencyManager latencyManager = new LatencyManager("pp", 1000000);

        // run test
        System.out.println("[BlockingPingPongSender] Running test (" + _testCount + " messages)...");
        int i = 0;
        long ts1, ts2;
        final long nanosPerMsg = _testRate > 0 ? (1000000000l / _testRate) : 0;
        long next = System.nanoTime() + nanosPerMsg;
        while (i < _testCount) {
            ts1 = System.nanoTime();
            if (ts1 >= next) {
                doPingPong(link, writeBuffers, readBuffer);
                ts2 = System.nanoTime();
                int latency = (int)(ts2 - ts1 - nanoTimeOverhead);
                if (_oneWay) latency /= 2;
                latencyManager.add(latency);
                i++;
                next += nanosPerMsg;
            }
        }
        System.out.println("[BlockingPingPongSender] Test complete.");

        // close link
        link.close();

        // compute and dump stats
        final StringBuilder sb = new StringBuilder();
        latencyManager.compute();
        sb.setLength(0);
        latencyManager.get(sb);
        System.out.print(sb.toString());
    }

    public static void main(String args[]) throws Exception {
        try {
            BlockingPingPongSender sender = new BlockingPingPongSender();
            sender.run(args);
        }
        catch (Exception e) {
            System.out.println("[BlockingPingPongSender] Received exception during benchmark run - " + e);
        }
    }
}
