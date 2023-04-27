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
import com.neeve.perf.common.SystemProperties;
import com.neeve.tools.interactive.commands.AnnotatedCommand;

@AnnotatedCommand.Command(keywords = "BlockingStreamingSender", description = "A blocking sender to test streaming performance using EMX links")
public class BlockingStreamingSender extends AnnotatedCommand {
    @Option(shortForm = 'd', longForm = "descriptor", required = true, description = "The connection descriptor to use e.g. tcp://192.168.1.7:12000&localifaddr=192.168.1.8&localport=12000&tcpnodelay=true")
    private String descriptor;

    @Option(shortForm = 'm', longForm = "messageSize", defaultValue = "256", required = true, description = "The size of the message to stream")
    private int messageSize;

    @Option(shortForm = 'b', longForm = "writeBufferSize", defaultValue = "8192", required = true, description = "The size of the connection's write buffer size")
    private int bufferSize;

    @Option(shortForm = 't', longForm = "testTime", defaultValue = "15", description = "The test time (post warmup), in seconds")
    private long testTime;

    @Option(shortForm = 'w', longForm = "warmUptime", defaultValue = "2", description = "The warm up time, in seconds")
    private int warmupTime;

    public void execute() throws Exception {
        // dump system props
        SystemProperties.dump();

        // establish connection
        System.out.println("[BlockingStreamingSender] Establishing link...");
        final EmxNwLnkConnector connector = EmxNwLnkConnector.create(descriptor);
        final EmxNwLnk link = connector.connect();

        // configure the established link
        System.out.println("[BlockingStreamingSender] Configuring link (message size=" + messageSize + ")...");
        link.configureBlockingWrite(true);

        // create the message write buffer
        final int messagesPerBuffer = (bufferSize / messageSize) + 1;
        final ByteBuffer[] buffers = new ByteBuffer[] {ByteBuffer.allocateDirect(messageSize * messagesPerBuffer)};

        // warm up
        System.out.println("[BlockingStreamingSender] Warming up (" + warmupTime + " seconds)...");
        final long warmupStart = System.currentTimeMillis();
        while ((System.currentTimeMillis() - warmupStart) < (warmupTime * 1000l)) {
            buffers[0].clear();
            link.write(buffers, 1);
        }

        // run test
        System.out.println("[BlockingStreamingSender] Running test (" + testTime + " seconds)...");
        final long start = System.currentTimeMillis();
        int numSent = 0;
        while ((System.currentTimeMillis() - start) < (testTime * 1000l)) {
            buffers[0].clear();
            link.write(buffers, 1);
            numSent += messagesPerBuffer;
        }
        System.out.println("[BlockingStreamingSender] Test complete.");

        // stats
        final long now = System.currentTimeMillis();
        final long overallRate = (numSent * 1000l) / (now - start);
        System.out.println("[BlockingStreamingSender] Sent " + numSent + " messages in " + testTime + " seconds.");
        System.out.println("[BlockingStreamingSender] Message rate is " + overallRate + " [msgs/sec].");

        // close link
        link.close();
    }

    public static void main(String args[]) throws Exception {
        try {
            BlockingStreamingSender sender = new BlockingStreamingSender();
            sender.run(args);
        }
        catch (Exception e) {
            System.out.println("[BlockingStreamingSender] Received exception during benchmark run - " + e);
        }
    }
}
