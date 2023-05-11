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
import com.neeve.link.ILnkContainerRunCompletionChecker;
import com.neeve.link.ILnkEndpoint;
import com.neeve.link.ILnkEventHandler;
import com.neeve.link.ILnkPeerEndpoint;
import com.neeve.link.ILnkSTRRootEndpoint;
import com.neeve.link.LnkEvents;
import com.neeve.link.LnkFactory;
import com.neeve.link.LnkRequest;
import com.neeve.link.LnkSTRRunnableContainer;
import com.neeve.link.LnkSender;
import com.neeve.tools.interactive.commands.AnnotatedCommand;

/**
 * Sender in the link performance test package.
 */

@AnnotatedCommand.Command(keywords = "PipelinedSender", description = "Starts a sender for pipelined sending performance")
final public class PipelinedSender extends Common {
    /*
     * The sender
     */
    final private class Sender extends LnkSTRRunnableContainer implements Runnable, ILnkEventHandler, ILnkContainerRunCompletionChecker {
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
         * The writer
         */
        final private class Writer extends Thread {
            /*
             * Send loop
             */
            final private void send() {
                /*
                 * Send
                 */
                System.out.println("<Sender#" + id + "> Sending packets...");
                final long start = System.nanoTime();
                long deltaStart = start;
                long deltaSend = 0;
                for (int k = 1; sendCount <= 0 || k <= sendCount; k++) {
                    /*
                     * Send
                     */
                    try {
                        ackPendingHigh++;
                        deltaSend++;
                        for (int i = 0; i < peps.length; i++) {
                            final LnkRequest request = senders[i].createRequest(packetManager.getPacketForSend());
                            senders[i].sendPipelined(request, null, 0);
                            request.detachPacket();
                        }
                        final long current = System.nanoTime();
                        if ((current - deltaStart) >= 1000000000L) {
                            final int overallThroughput = (int)(((double)ackPendingHigh * 1000000000L) / (current - start));
                            final int deltaThroughput = (int)(((double)deltaSend * 1000000000L) / (current - deltaStart));
                            System.out.println("Throughput = " + overallThroughput + "(" + deltaThroughput + ") UnAck Window=" + (ackPendingHigh - ackPendingLow));
                            deltaStart = System.nanoTime();
                            deltaSend = 0;
                        }
                    }
                    catch (Exception e) {
                        System.out.println("<Sender#" + id + "> Send failure [" + e.toString() + "].");
                        break;
                    }
                }
                for (int i = 0; i < peps.length; i++) {
                    try {
                        peps[i].flush((short)-1, null);
                    }
                    catch (Exception e) {}
                }

                /*
                 * Done
                 */
                System.out.println("<Sender#" + id + "> Sending done.");
            }

            /*
             * Entry point
             */
            final public void run() {
                send();
            }
        }

        /*
         * Private members
         */
        final private int id;
        final private String[] descs;
        final private int sendCount;
        final private ILnkClientEndpoint[] ceps;
        final private ConnectCompleteEventHandler[] connectCompleteHandlers;
        private ILnkPeerEndpoint[] peps;
        private LnkSender[] senders;
        private boolean done = false;
        private volatile long ackPendingLow = 0;
        private volatile long ackPendingHigh = 0;

        /*
         * Constructor
         */
        Sender(final int id,
               final String[] descs,
               final int sendCount) throws Exception {
            /*
             * STR container
             */
            super(null,
                  "Sender#" + id,
                  EmxFactory.getInstance().createDispatcher(EmxFactory.EmxImpl.DEFAULT, "Sender#" + id, null),
                  0);

            /*
             * Store supplied parameters
             */
            this.id = id;
            this.sendCount = sendCount;

            /*
             * Create the client endpoint and connect complete handler for 
             * each of the descriptors
             */
            this.descs = new String[descs.length];
            this.ceps = new ILnkClientEndpoint[this.descs.length];
            this.connectCompleteHandlers = new ConnectCompleteEventHandler[this.descs.length];
            for (int i = 0; i < this.descs.length; i++) {
                final String desc = this.descs[i] = touch(descs[i]) + "&maxusers=1&threadingmodel=strstw";
                try {
                    this.ceps[i] = LnkFactory.getInstance().createClientEndpoint(desc, null);
                }
                catch (Exception e) {
                    System.out.println("<Sender#" + id + "> Failed to create client endpoint [error=" + e.toString() + "].");
                    throw e;
                }

                /*
                 * Create the connect complete handler
                 */
                this.connectCompleteHandlers[i] = new ConnectCompleteEventHandler();

                /*
                 * Trace
                 */
                System.out.println("<Sender#" + id + "> Created with [id=" + id + " desc='" + this.descs[i] + "].");
            }

            /*
             * Create the remaining member objects
             */
            this.peps = new ILnkPeerEndpoint[this.descs.length];
            this.senders = new LnkSender[this.descs.length];
        }

        /*
         * Connect
         */
        final private boolean connect() {
            /*
             * Connect
             */
            for (int i = 0; i < descs.length; i++) {
                /*
                 * Get next descriptor
                 */
                final String desc = descs[i];

                /*
                 * Connect
                 */
                try {
                    System.out.println("<Sender#" + id + "> Connecting [desc=" + desc + "]...");
                    ceps[i].connectPost(reader, connectCompleteHandlers[i], -1, 0);
                    while (connectCompleteHandlers[i].eventData == null) {
                        reader.run(-1);
                    }

                    /*
                     * Connect complete
                     */
                    if (connectCompleteHandlers[i].eventData.status) {
                        System.out.println("<Sender#" + id + "> Connect success.");
                        peps[i] = connectCompleteHandlers[i].eventData.pep;

                        /*
                         * Join, create the link sender and request objects and start read machinery.
                         */
                        try {
                            try {
                                peps[i].join((short)-1, this);
                            }
                            catch (Exception e) {
                                System.out.println("<Sender#" + id + "> Join failure [" + e.toString() + "].");
                            }

                            try {
                                senders[i] = LnkSender.create(peps[i]);
                                senders[i].open();
                            }
                            catch (Exception e) {
                                System.out.println("<Sender#" + id + "> Sender create/open failure [" + e.toString() + "].");
                                throw e;
                            }

                            try {
                                ((ILnkSTRRootEndpoint)peps[i].getRootEndpoint()).startRead(reader, 0);
                            }
                            catch (Exception e) {
                                System.out.println("<Sender#" + id + "> Read machinery start failure [" + e.toString() + "].");
                            }
                        }
                        catch (Exception e) {
                            try {
                                peps[i].close((short)-1);
                            }
                            catch (Exception e1) {}
                            return false;
                        }
                    }
                    else {
                        throw connectCompleteHandlers[i].eventData.e;
                    }
                }
                catch (Exception e) {
                    System.out.println("<Sender#" + id + "> Connect failure [" + e.toString() + "].");
                    try {
                        ceps[i].close();
                    }
                    catch (Exception e1) {}
                    return false;
                }
            }
            return true;
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
                     * Start the writer
                     */
                    new Writer().start();

                    /*
                     * Drive the reader
                     */
                    run(-1, this);

                    /*
                     * Done
                     */
                    System.out.println("<Sender#" + id + "> Receving done.");
                }
            }
            catch (Exception e) {
                System.out.println("<Sender#" + id + "> Unhandled fault [" + e.toString() + "]");
            }
        }

        /**
         * Implementation of {@link ILnkEventHandler#onEvent}
         */
        final public void onEvent(final IEmxDispatcher dispatcher,
                                  final ILnkEndpoint ep,
                                  final int event,
                                  final Object data) {
            if (event == LnkEvents.EVENT_REQUEST_COMPLETE) {
                final LnkRequest request = (LnkRequest)data;
                final long corrid = request.getCorrelationId();
                if (corrid != ackPendingLow + 1) {
                    throw new InternalError("Received out of order ack [exp=" + (ackPendingLow + 1) + " actual=" + corrid + "]!");
                }
                ackPendingLow = corrid;
                done = ackPendingLow == sendCount;
            }
            else if (event == LnkEvents.EVENT_FAILURE) {
                System.out.println("<Sender#" + id + "> Link failure [" + ((Exception)data).toString() + "]");
                done = true;
            }
            else {
                throw new InternalError("<Sender#" + id + "> Received unexpected event [type=" + event + " data=" + data + "] through pipelined sender event handler!");
            }
        }

        /**
         * Implementation of {@link ILnkContainerRunCompletionChecker#isDone}
         */
        public boolean isDone() {
            return done;
        }

        /**
         * Implementation of {@link ILnkContainerRunCompletionChecker#getCompletion}
         */
        public Object getCompletion() throws Exception {
            return null;
        }

    }

    /*
     * Private members
     */
    @RemainingArgs(name = "descriptors", required = true, description = "A space separated set of sender address descriptors to create")
    String[] descs;

    @Option(shortForm = 'c', longForm = "sendCount", required = true, defaultValue = "0", description = "The number of sends to peform")
    int sendCount;

    @Option(shortForm = 't', longForm = "threads", required = true, defaultValue = "1", description = "The number of send threads to start")
    private int numThreads;

    /*
     * Constructor
     */
    public PipelinedSender() throws Exception {
        super();
    }

    @Override
    final public void doRun() throws Exception {
        /*
         * Create the senders
         */
        final Sender[] senders = new Sender[numThreads];
        for (int i = 0; i < numThreads; i++) {
            senders[i] = new Sender(i + 1, descs, sendCount);
        }

        /*
         * Start the senders
         */
        for (int i = 0; i < numThreads; i++) {
            new Thread(senders[i]).start();
        }
    }

    public static void main(String args[]) throws Exception {
        new PipelinedSender().run(args);
    }
}
