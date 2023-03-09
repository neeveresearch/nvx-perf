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
import com.neeve.tools.interactive.commands.AnnotatedCommand;
import com.neeve.perf.common.SystemProperties;

@AnnotatedCommand.Command(keywords = "BlockingStreamingSender", description = "A blocking sender to test streaming performance using EMX links")
public class BlockingStreamingSender extends AnnotatedCommand {
    @Option(shortForm = 'd', longForm = "descriptor", required = true, description = "The connection descriptor to use e.g. tcp://192.168.1.7:12000&localifaddr=192.168.1.8&localport=12000&tcpnodelay=true")
    String descriptor;

    @Option(shortForm = 's', longForm = "serializedMessageSize", defaultValue = "256", required = true, description = "The size of serialized message")
    int serializedMessageSize;

    @Option(shortForm = 'b', longForm = "writeBufferSize", defaultValue = "8192", required = true, description = "The size of the write buffer size")
    int bufferSize;

    @Option(shortForm = 'c', longForm = "count", defaultValue = "10000000", description = "The number of packets to send")
    long count;

    public void execute() throws Exception {
        SystemProperties.dump();
        final int messagesPerBuffer = (bufferSize / serializedMessageSize) + 1;
        final ByteBuffer[] buffers = new ByteBuffer[] {ByteBuffer.allocateDirect(serializedMessageSize * messagesPerBuffer)};
        final EmxNwLnkConnector connector = EmxNwLnkConnector.create(descriptor);
        final EmxNwLnk link = connector.connect();
        link.configureBlockingWrite(true);
        final long start = System.currentTimeMillis();
        int numSent = 0;
        int deltaNumSent = 0;
        long deltaStart = start;
        while (numSent < count) {
            buffers[0].clear();
            link.write(buffers, 1);
            numSent += messagesPerBuffer;
            deltaNumSent += messagesPerBuffer; 
            final long now = System.currentTimeMillis();
            if (now - deltaStart >= 1000l) {
                final long deltaRate = (deltaNumSent * 1000l) / (now - deltaStart);
                final long overallRate = (numSent * 1000l) / (now - start);
                System.out.println("RATE [" + deltaRate + "," + overallRate + "]");
                deltaStart = now;
                deltaNumSent = 0;
            }
        }
        link.close();
    }

    public static void main(String args[]) throws Exception {
        try {
            BlockingStreamingSender sender = new BlockingStreamingSender();
            sender.run(args);
        }
        catch (Exception e) {
            System.out.println("Received exception during benchmark run - " + e);
        }
    }
}
