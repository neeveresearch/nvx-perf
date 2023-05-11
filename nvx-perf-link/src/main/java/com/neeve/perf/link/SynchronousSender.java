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
package com.neeve.perf.link;

import com.neeve.emx.EmxFactory;
import com.neeve.emx.IEmxDispatcher;
import com.neeve.link.ILnkClientEndpoint;
import com.neeve.link.ILnkEndpoint;
import com.neeve.link.ILnkEventHandler;
import com.neeve.link.ILnkPeerEndpoint;
import com.neeve.link.ILnkSTRRootEndpoint;
import com.neeve.link.LnkEvents;
import com.neeve.link.LnkFactory;
import com.neeve.link.LnkRequest;
import com.neeve.link.LnkSTRContainer;
import com.neeve.link.LnkSender;
import com.neeve.pkt.PktPacket;
import com.neeve.tools.interactive.commands.AnnotatedCommand;

/**
 * Synchronous sender in the link performance test package.
 */
@AnnotatedCommand.Command(keywords = "SynchronousLinkSender", description = "Starts a client for testing network roundtrip performances")
final public class SynchronousSender extends Common {

    /* 
     * The pinger
     */
    final private class Pinger extends LnkSTRContainer implements Runnable, ILnkEventHandler {
        /*
         * Connect complete event handler
         */
        final private class ConnectCompleteEventHandler implements ILnkEventHandler {
            /*
             * Private scope members
             */
            LnkEvents.ConnectAcceptCompleteEventData eventData;

            /**
             * Implementation of {@link ILnkEventHandler#onEvent}
             */
            final public void onEvent(final IEmxDispatcher dispatcher,
                                      final ILnkEndpoint ep,
                                      final int event,
                                      final Object data) {
                this.eventData = (LnkEvents.ConnectAcceptCompleteEventData)data;
            }
        }

        /*
         * Private members
         */
        final private int id;
        final private String desc;
        final private int pingCount;
        private ILnkPeerEndpoint pep;
        private boolean done = false;

        /*
         * Constructor
         */
        Pinger(final int id,
               final String desc,
               final int pingCount) throws Exception {
            /*
             * STR container
             */
            super(null,
                  "Pinger#" + id,
                  EmxFactory.getInstance().createDispatcher(EmxFactory.EmxImpl.DEFAULT,
                                                            "Pinger#" + id,
                                                            null),
                  0);

            /*
             * Store id, desc and data size
             */
            this.id = id;
            this.desc = touch(desc) + "&maxusers=1";
            this.pingCount = pingCount;

            /*
             * Trace
             */
            System.out.println("<Pinger#" + id + "> Created with [id=" + id + " desc='" + this.desc + "'].");
        }

        /*
         * Connect
         */
        final private boolean connect() {
            /*
             * Create the client endpoint
             */
            ILnkClientEndpoint cep;
            try {
                cep = LnkFactory.getInstance().createClientEndpoint(this.desc, null);
            }
            catch (Exception e) {
                System.out.println("<Pinger#" + id + "> Failed to create client endpoint [error=" + e.toString() + "].");
                return false;
            }

            /*
             * Connect
             */
            try {
                final ConnectCompleteEventHandler connectCompleteHandler = new ConnectCompleteEventHandler();
                System.out.println("<Pinger#" + id + "> Connecting [desc=" + desc + "]...");
                cep.connectPost(reader, connectCompleteHandler, -1, 0);
                while (connectCompleteHandler.eventData == null) {
                    reader.run(-1);
                }

                /*
                 * Connect complete
                 */
                if (connectCompleteHandler.eventData.status) {
                    System.out.println("Connect success.");
                    pep = connectCompleteHandler.eventData.pep;
                }
                else {
                    throw connectCompleteHandler.eventData.e;
                }
            }
            catch (Exception e) {
                System.out.println("<Pinger#" + id + "> Connect failure [" + e.toString() + "].");
                try {
                    cep.close();
                }
                catch (Exception e1) {}
                return false;
            }

            /*
             * Join
             */
            try {
                pep.join((short)-1, this);
            }
            catch (Exception e) {
                System.out.println("<Pinger#" + id + "> Join failure [" + e.toString() + "].");
                try {
                    pep.close((short)-1);
                }
                catch (Exception e1) {}
                return false;
            }

            /*
             * Start the read machinery
             */
            try {
                System.out.println("<Pinger#" + id + "> Starting read machinery...");
                ((ILnkSTRRootEndpoint)pep.getRootEndpoint()).startRead(reader, 0);
            }
            catch (Exception e) {
                System.out.println("<Pinger#" + id + "> Read machinery start failure [" + e.toString() + "].");
                return false;
            }

            return true;
        }

        /*
         * Send loop
         */
        final private void send() {
            LnkSender sender = null;
            try {
                /*
                 * Create and open the link sender
                 */
                try {
                    (sender = LnkSender.create(pep)).open();
                }
                catch (Exception e) {
                    System.out.println("<Pinger#" + id + "> Failed to instantiate a link sender [" + e.toString() + "]");
                    return;
                }

                /*
                 * Create a sender request object.
                 */
                final LnkRequest request = sender.createRequest(null);

                /*
                 * Send
                 */
                System.out.println("<Pinger#" + id + "> Sending packets...");
                final long startTime = System.nanoTime();
                long deltaStartTime = startTime;
                long statsStartTime = 0;
                int warmUpCount = pingCount / 2;
                long totalRtt = 0;
                long totalDeltaRtt = 0;
                int seqNo = 1;
                int deltaSeqNo = seqNo;
                for (; !done && seqNo <= pingCount; seqNo++, deltaSeqNo++) {
                    try {
                        final long sendTime = System.nanoTime();
                        final PktPacket packet = packetManager.getPacketForSend();
                        sender.sendSync(request.init(packet), 5000);
                        final long recvTime = System.nanoTime();
                        packet.dispose();
                        long rttTime = (recvTime - sendTime);
                        totalRtt += rttTime;
                        totalDeltaRtt += rttTime;
                        if ((recvTime - deltaStartTime) > 1000000000L) {
                            long throughput = (seqNo * 1000000000L) / (recvTime - startTime);
                            long deltaThroughput = (deltaSeqNo * 1000000000L) / (recvTime - deltaStartTime);
                            long rtt = totalRtt / seqNo;
                            long deltaRtt = totalDeltaRtt / deltaSeqNo;
                            System.out.println("<Pinger#" + id + "> Throughput= " + throughput + "(" + deltaThroughput + ") Latency=" + rtt + "(" + (deltaRtt / 1000) + "usec)");
                            deltaSeqNo = 0;
                            deltaStartTime = recvTime;
                            totalDeltaRtt = 0;
                        }
                    }
                    catch (Exception e) {
                        System.out.println("<Pinger#" + id + "> Send failure [" + e.toString() + "].");
                        break;
                    }
                    if (seqNo == warmUpCount) {
                        statsStartTime = System.nanoTime();
                        System.out.println("<Pinger#" + id + "> Started stats collection.");
                    }
                }
                final long endTime = System.nanoTime();
                final int statsCount = (pingCount - warmUpCount);
                System.out.println("<Pinger#" + id + "> Overall Throughput (statsCount=" + statsCount + ") = " + ((statsCount * 1000000000L) / (endTime - statsStartTime)));
            }
            finally {
                try {
                    if (sender != null) {
                        sender.close();
                    }
                    pep.close((short)-1);
                }
                catch (Exception e1) {}
            }
        }

        /*
         * Thread entry point
        */
        final public void run() {
            try {
                /*
                 * Take ownership of reader thread
                 */
                reader.setOwner();

                /*
                 * Connect
                 */
                if (connect()) {
                    /*
                     * Send packets
                     */
                    send();
                }
            }
            catch (Exception e) {
                System.out.println("<Pinger#" + id + "> Unhandled fault [" + e.toString() + "]");
            }
        }

        /**
         * Implementation of {@link ILnkEventHandler#onEvent}
         */
        final public void onEvent(final IEmxDispatcher dispatcher,
                                  final ILnkEndpoint ep,
                                  final int event,
                                  final Object data) {
            if (event == LnkEvents.EVENT_FAILURE) {
                System.out.println("<Pinger#" + id + "> Link failure [" + ((Exception)data).toString() + "]");
                done = true;
            }
            else {
                throw new InternalError("<Pinger#" + id + "> Received event [type=" + event + " data=" + data + "] through performance sender event handler!");
            }
        }
    }

    /*
     * Options:
     */

    @Option(shortForm = 'p', longForm = "peerAddress", required = true, description = "The descriptor to use for connect")
    String desc;

    @Option(shortForm = 's', longForm = "size", required = true, defaultValue = "100", description = "The size of the packet to send in bytes")
    int packetSize;

    @Option(shortForm = 'c', longForm = "pingCount", required = true, defaultValue = "1", description = "The number of pings to send")
    int pingCount;

    @Option(shortForm = 't', longForm = "threads", required = true, defaultValue = "1", description = "The number of send threads to start")
    private int numThreads;

    /*
     * Constructor
     */
    public SynchronousSender() throws Exception {
        super();
    }

    @Override
    final public void doRun() throws Exception {
        /*
         * Create the threads
         */
        final Pinger[] pingers = new Pinger[numThreads];
        for (int i = 0; i < numThreads; i++) {
            pingers[i] = new Pinger(i + 1, desc, pingCount);
        }

        /*
         * Start the threads
         */
        for (int i = 0; i < numThreads; i++) {
            new Thread(pingers[i]).start();
        }
    }

    public static void main(String args[]) throws Exception {
        try {
            new SynchronousSender().run(args);
        }
        catch (Throwable e) {
            System.out.println("Unhandled fault [" + e.toString() + "]");
            e.printStackTrace();
        }
    }
}
