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

import com.neeve.emx.EmxNwLnkAcceptor;
import com.neeve.emx.EmxNwLnkReader;
import com.neeve.emx.EmxNwLnkNonBlockingReader;
import com.neeve.emx.EmxNwLnk;
import com.neeve.io.IOBuffer;
import com.neeve.perf.common.SystemProperties;
import com.neeve.tools.interactive.commands.AnnotatedCommand;
import com.neeve.util.UtlThread;

@AnnotatedCommand.Command(keywords = "NonBlockingStreamingReceiver", description = "A non-blocking receiver to test streaming performance using EMX links")
public class NonBlockingStreamingReceiver extends AnnotatedCommand {
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
        private long start;
        private long deltaStart;
        private int numRcvd;
        private int deltaNumRcvd;

        @Override
        final public int handleReadData(final EmxNwLnk lnk, final IOBuffer iobuf, final int length) {
            // get count received
            final int count = length / _messageSize;

            // stats
            if (_stats) {
                final long now = System.currentTimeMillis();
                if (start == 0l) {
                    start = deltaStart = now;
                }
                numRcvd += count;
                deltaNumRcvd += count;
                if (now - deltaStart >= 1000l) {
                    final long deltaRate = (deltaNumRcvd * 1000l) / (now - deltaStart);
                    final long overallRate = (numRcvd * 1000l) / (now - start);
                    System.out.println("[NonBlockingStreamingReceiver] RATE [" + deltaRate + "," + overallRate + "]");
                    deltaStart = now;
                    deltaNumRcvd = 0;
                }
            }

            // done
            return _messageSize * count; 
        }

        @Override
        final public void handleLinkClosure(final EmxNwLnk lnk) {
            System.out.println("[NonBlockingStreamingReceiver] Link closed by peer");
        }

        @Override
        final public void handleLinkFailure(final EmxNwLnk lnk, final Throwable cause) {
            cause.printStackTrace();
        }
    }

    final private class ReaderThread extends Thread {
        ReaderThread() throws Exception {
            super(reader);
        }

        @Override
        final public void run() {
            // affinitize to CPU
            if (_cpuAffinityMask != null) {
                System.out.println("[NonBlockingStreamingReceiver] Affinitizing thread to CPU " + _cpuAffinityMask);
                UtlThread.setCPUAffinityMask(UtlThread.parseAffinityMask(_cpuAffinityMask));
            }

            // run
            super.run();
        }
    }

    @Option(shortForm = 'd', longForm = "descriptor", required = true, description = "The connection descriptor to use e.g. tcp://192.168.1.7:12000&tcpnodelay=true")
    private String _descriptor;

    @Option(shortForm = 'm', longForm = "messageSize", defaultValue = "256", required = true, description = "The size of the message being streamed")
    private int _messageSize;

    @Option(shortForm = 'c', longForm = "cpuAffinityMask", description = "the CPU() to affinitize the reading thread to")
    private String _cpuAffinityMask;

    @Option(shortForm = 's', longForm = "stats", defaultValue = "false", required = true, description = "Whether to output incremental throughput stats")
    private boolean _stats;

    private EmxNwLnkNonBlockingReader reader;

    public void execute() throws Exception {
        SystemProperties.dump();
        final EmxNwLnkAcceptor acceptor = EmxNwLnkAcceptor.create(_descriptor, new AcceptCallback());
        this.reader = EmxNwLnkNonBlockingReader.create(new ReadCallback());
        new ReaderThread().start();
        new Thread(acceptor).run();
    }

    public static void main(String args[]) throws Exception {
        try {
            NonBlockingStreamingReceiver receiver = new NonBlockingStreamingReceiver();
            receiver.run(args);
        }
        catch (Exception e) {
            System.out.println("[NonBlockingStreamingReceiver] Received exception during benchmark run - " + e);
        }
    }
}
