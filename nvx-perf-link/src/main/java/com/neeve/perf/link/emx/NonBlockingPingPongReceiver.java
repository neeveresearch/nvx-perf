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

import com.neeve.emx.EmxNwLnkAcceptor;
import com.neeve.emx.EmxNwLnkReader;
import com.neeve.emx.EmxNwLnkNonBlockingReader;
import com.neeve.emx.EmxNwLnk;
import com.neeve.io.IOBuffer;
import com.neeve.tools.interactive.commands.AnnotatedCommand;
import com.neeve.perf.common.SystemProperties;

@AnnotatedCommand.Command(keywords = "NonBlockingPingPongReceiver", description = "A non-blocking receiver to test ping pong performance using EMX links")
public class NonBlockingPingPongReceiver extends AnnotatedCommand {
    final private class AcceptCallback implements EmxNwLnkAcceptor.Callback {
        @Override
        final public void handleAcceptedLink(final EmxNwLnk link) {
            try {
                System.out.println("Accepted a new connection");
                reader.addLink(link); 
            }
            catch (Throwable e) {
                e.printStackTrace();
            }
        }

        @Override
        final public void handleAcceptorFailure(final Throwable cause) {
            cause.printStackTrace();
        }
    }

    final private class ReadCallback implements EmxNwLnkReader.Callback {
        final private ByteBuffer[] writeBuffers = new ByteBuffer[] {ByteBuffer.allocateDirect(serializedMessageSize)};
        private long start;
        private long deltaStart;
        private int numRcvd;
        private int deltaNumRcvd;

        @Override
        final public int handleReadData(final EmxNwLnk link, final IOBuffer iobuf, final int length) {
            final int count = length / serializedMessageSize;
            try {
                if (count > 0) {
                    for (int i = 0; i < count; i++) {
                        writeBuffers[0].clear();
                        writeBuffers[0].limit(serializedMessageSize);
                        link.write(writeBuffers, 1);
                    }
                    final long now = System.currentTimeMillis(); 
                    if (start == 0l) {
                        start = deltaStart = now;
                    }
                    numRcvd += count;
                    deltaNumRcvd += count;
                    if (now - deltaStart >= 1000l) {
                        final long deltaRate = (deltaNumRcvd * 1000l) / (now - deltaStart);
                        final long overallRate = (numRcvd * 1000l) / (now - start);
                        System.out.println("RATE [" + deltaRate + "," + overallRate + "]");
                        deltaStart = now;
                        deltaNumRcvd = 0;
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

    @Option(shortForm = 'd', longForm = "descriptor", required = true, description = "The connection descriptor to use e.g. tcp://192.168.1.7:12000&tcpnodelay=true")
    String descriptor;

    @Option(shortForm = 's', longForm = "serializedMessageSize", defaultValue = "256", required = true, description = "The size of the packet to send")
    int serializedMessageSize;

    private EmxNwLnkNonBlockingReader reader;

    public void execute() throws Exception {
        SystemProperties.dump();
        final EmxNwLnkAcceptor acceptor = EmxNwLnkAcceptor.create(descriptor, new AcceptCallback());
        new Thread(reader = EmxNwLnkNonBlockingReader.create(new ReadCallback())).start();
        new Thread(acceptor).run();
    }

    public static void main(String args[]) throws Exception {
        try {
            NonBlockingPingPongReceiver sender = new NonBlockingPingPongReceiver();
            sender.run(args);
        }
        catch (Exception e) {
            System.out.println("Received exception during benchmark run - " + e);
        }
    }
}
