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

import com.neeve.emx.EmxNwLnkConnector;
import com.neeve.emx.EmxNwLnk;
import com.neeve.perf.common.SystemProperties;
import com.neeve.stats.Stats.LatencyManager;
import com.neeve.tools.interactive.commands.AnnotatedCommand;
import com.neeve.util.UtlConstants;
import com.neeve.util.UtlThread;

@AnnotatedCommand.Command(keywords = "BlockingStreamingSender", description = "A blocking sender to test streaming performance using EMX links")
public class BlockingStreamingSender extends AnnotatedCommand {
    @Option(shortForm = 'd', longForm = "descriptor", required = true, description = "The connection descriptor to use e.g. tcp://192.168.1.7:12000&localifaddr=192.168.1.8&localport=12000&tcpnodelay=true")
    private String _descriptor;

    @Option(shortForm = 'm', longForm = "messageSize", defaultValue = "256", required = true, description = "The size of the message to stream")
    private int _messageSize;

    @Option(shortForm = 'b', longForm = "bufferSize", defaultValue = "256", required = true, description = "The size of the connection's write buffer size")
    private int _bufferSize;

    @Option(shortForm = 't', longForm = "testCount", defaultValue = "100000000", description = "The test count")
    private int _testCount;

    @Option(shortForm = 'r', longForm = "testRate", defaultValue = "0", description = "The send rate")
    private int _testRate;

    @Option(shortForm = 'w', longForm = "warmupTime", defaultValue = "2", description = "The warm up time, in seconds")
    private int _warmupTime;

    @Option(shortForm = 'c', longForm = "cpuAffinityMask", description = "which CPU(s) to affinitize the sending thread to")
    private String _cpuAffinityMask;

    public void execute() throws Exception {
        // affinitize to CPU
        if (_cpuAffinityMask != null) {
            System.out.println("[BlockingStreamingSender] Affinitizing thread to CPU " + _cpuAffinityMask);
            UtlThread.setCPUAffinityMask(UtlThread.parseAffinityMask(_cpuAffinityMask));
        }

        // dump system props
        SystemProperties.dump();

        // dump test parameters
        DecimalFormat dfmt = new DecimalFormat("#,###");
        System.out.println("[BlockingStreamingSender] Descriptor:" + _descriptor);
        System.out.println("[BlockingStreamingSender] Message size:" + _messageSize);
        System.out.println("[BlockingStreamingSender] Buffer size:" + _bufferSize);
        System.out.println("[BlockingStreamingSender] Test count:" + dfmt.format(_testCount));
        System.out.println("[BlockingStreamingSender] Warmup time:" + _warmupTime + "s");
        System.out.println("[BlockingStreamingSender] Test rate:" + dfmt.format(_testRate));
        System.out.println("[BlockingStreamingSender] CPU affinity mask:" + _cpuAffinityMask);

        // establish connection
        System.out.println("[BlockingStreamingSender] Establishing link...");
        final EmxNwLnkConnector connector = EmxNwLnkConnector.create(_descriptor);
        final EmxNwLnk link = connector.connect();

        // configure the established link
        System.out.println("[BlockingStreamingSender] Configuring link (message size=" + _messageSize + ")...");
        link.configureBlockingWrite(true);

        // create the message write buffer
        final int messagesPerBuffer = (_bufferSize / _messageSize) + ((_bufferSize % _messageSize) > 0 ? 1 : 0);
        final ByteBuffer[] buffers = new ByteBuffer[] {ByteBuffer.allocateDirect(_messageSize * messagesPerBuffer)};

        // calculate nanoTime overhead
        System.out.println("[BlockingStreamingSender] Calculating nanoTime overhead...");
        long nanoTimeOverhead = 0l;
        long start = System.nanoTime();
        for (int i = 0; i < 100000000l; i++) {
            System.nanoTime();
        }
        nanoTimeOverhead = (System.nanoTime() - start) / 100000000l;
        System.out.println("[BlockingStreamingSender] ..." + nanoTimeOverhead + "ns");

        // create latency manager
        final LatencyManager latencyManager = new LatencyManager("pp", 1000000);

        // warm up
        System.out.println("[BlockingStreamingSender] Warming up (" + _warmupTime + " seconds)...");
        final long warmupStart = System.currentTimeMillis();
        while ((System.currentTimeMillis() - warmupStart) < (_warmupTime * 1000l)) {
            buffers[0].clear();
            link.write(buffers, 1);
        }

        // run test
        int numSent = 0;
        long ts1, ts2;
        final int bufferRate = _testRate / messagesPerBuffer;
        final int bufferCount = _testCount / messagesPerBuffer;
        final long nanosPerMsg = bufferRate > 0 ? (1000000000l / bufferRate) : 0;
        System.out.println("[BlockingStreamingSender] Running test (" + dfmt.format(_testCount) + " messages)...");
        System.out.println("[BlockingStreamingSender] ...BufferSize=" + buffers[0].remaining() + " (" + messagesPerBuffer + " msgs/buffer)");
        System.out.println("[BlockingStreamingSender] ...BufferCount=" + dfmt.format(bufferCount));
        System.out.println("[BlockingStreamingSender] ...BufferRate=" + dfmt.format(bufferRate) + "/sec");
        long next = System.nanoTime() + nanosPerMsg;
        start = System.currentTimeMillis();
        while (numSent < _testCount) {
            ts1 = System.nanoTime();
            if (ts1 >= next) {
                buffers[0].clear();
                link.write(buffers, 1);
                ts2 = System.nanoTime();
                final int writeTime = (int)(ts2 - ts1 - nanoTimeOverhead);
                latencyManager.add(writeTime);
                next += nanosPerMsg;
                numSent += messagesPerBuffer;
            }
        }
        System.out.println("[BlockingStreamingSender] Test complete.");

        // close link
        link.close();

        // stats
        final long now = System.currentTimeMillis();
        final long overallRate = (numSent * 1000l) / (now - start);
        System.out.println("[BlockingStreamingSender] Sent " + dfmt.format(numSent) + " messages at " + dfmt.format(overallRate) + " msgs/sec.");

        // compute and dump stats
        final StringBuilder sb = new StringBuilder();
        latencyManager.compute();
        sb.setLength(0);
        latencyManager.get(sb);
        System.out.print(sb.toString());
    }

    public static void main(String args[]) throws Exception {
        try {
            System.setProperty(UtlConstants.THREAD_ENABLECPUAFFINITYMASKS_PROPNAME, "true");
            BlockingStreamingSender sender = new BlockingStreamingSender();
            sender.run(args);
        }
        catch (Exception e) {
            System.out.println("[BlockingStreamingSender] Received exception during benchmark run - " + e);
        }
    }
}
