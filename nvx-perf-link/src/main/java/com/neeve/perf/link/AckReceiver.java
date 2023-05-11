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
import com.neeve.link.ILnkEndpoint;
import com.neeve.link.ILnkEventHandler;
import com.neeve.link.ILnkPeerEndpoint;
import com.neeve.link.ILnkSTRRootEndpoint;
import com.neeve.link.ILnkServerEndpoint;
import com.neeve.link.LnkEvents;
import com.neeve.link.LnkFactory;
import com.neeve.pkt.PktFactory;
import com.neeve.pkt.PktPacket;
import com.neeve.pkt.PktSubheaderRR;
import com.neeve.pkt.types.PktBodyTypesBase;
import com.neeve.tools.interactive.commands.AnnotatedCommand;

/**
 * Receiver in the link performance test package.
 */
@AnnotatedCommand.Command(keywords = "AckReceiver", description = "Starts a receiver for testing network roundtrip performance")
final public class AckReceiver extends Common {
    /*
     * Accept complete event handler
     */
    final private class AcceptCompleteEventHandler implements ILnkEventHandler {
        /*
         * Private scope members
         */
        LnkEvents.ConnectAcceptCompleteEventData eventData;

        /*
         * Reset
         */
        final void reset() {
            this.eventData = null;
        }

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
     * Acker
     */
    final private class Acker extends Thread implements ILnkEventHandler {
        /*
         * Private scope members
         */
        final private int id;
        final private ILnkPeerEndpoint pep;
        private boolean done = false;

        /*
         * Constructor
         */
        Acker(final int id, final ILnkPeerEndpoint pep) {
            this.id = id;
            this.pep = pep;
        }

        /*
         * Thread entry point
         */
        final public void run() {
            /*
             * Create the dispatcher to drive this link
             */
            IEmxDispatcher dispatcher;
            try {
                dispatcher = EmxFactory.getInstance().createDispatcher(EmxFactory.EmxImpl.DEFAULT,
                                                                       ("Acker#" + id),
                                                                       null);
            }
            catch (Exception e) {
                System.out.println("<Acker#" + id + "> Dispatcher create failure [" + e.toString() + "].");
                return;
            }

            /*
             * Join link
             */
            try {
                pep.join((short)-1, this);
            }
            catch (Exception e) {
                System.out.println("<Acker#" + id + "> Join failure [" + e.toString() + "].");
                try {
                    pep.close((short)-1);
                }
                catch (Exception e1) {}
                return;
            }

            /*
             * Start the read machinery
             */
            try {
                System.out.println("<Acker#" + id + "> Starting read machinery...");
                ((ILnkSTRRootEndpoint)pep.getRootEndpoint()).startRead(dispatcher, 0);
            }
            catch (Exception e) {
                System.out.println("<Acker#" + id + "> Read machinery start failure [" + e.toString() + "].");
                return;
            }

            /*
             * Dispatch
             */
            try {
                /*
                 * Dispatch
                 */
                System.out.println("<Acker#" + id + "> Receiving packets...");
                while (!done) {
                    dispatcher.run(-1);
                }
            }
            finally {
                try {
                    pep.close((short)-1);
                }
                catch (Exception e1) {}
            }
        }

        /*
         * Prepare and write packet
         */
        final private void write(final PktPacket request, final boolean flush) {
            try {
                final PktPacket reply = PktFactory.getInstance().createPacket(PktBodyTypesBase.REPLY);
                try {
                    if (PktSubheaderRR.prepareReply(request, reply, 1, 1)) {
                        pep.enque((short)-1, reply, null, flush ? ILnkPeerEndpoint.IOFLAG_FLUSH_FORCE : 0);
                    }
                    else {
                        System.out.println("<Acker#" + id + "> Failure (in sending reply) [inbound packet is not a request!]");
                    }
                }
                finally { 
                    reply.dispose();
                }
            }
            catch (Exception e) {
                System.out.println("<Acker#" + id + "> Failure (in sending reply) [" + e.toString() + "]");
                done = true;
            }
        }

        /**
         * Implementation of {@link ILnkEventHandler#onEvent}
         */
        final public void onEvent(final IEmxDispatcher dispatcher,
                                  final ILnkEndpoint ep,
                                  final int event,
                                  final Object data) {
            if (event == LnkEvents.EVENT_PACKET) {
                final PktPacket packet = (PktPacket)data;
                packetManager.onReceive(packet);
                write(packet, true);
                packet.dispose();
            }
            else if (event == LnkEvents.EVENT_FAILURE) {
                System.out.println("<Acker#" + id + "> Link failure [" + ((Exception)data).toString() + "]");
                done = true;
            }
        }
    }

    /*
     * Private members
     */
    @Argument(position = 1, name = "descriptor", required = true, description = "The server descriptor to use.")
    private String desc;

    /*
     * Constructor
     */
    private AckReceiver() throws Exception {
        super();
    }

    /*
     * Accept new incoming connections
     */
    final private void accept() throws Exception {
        /*
         * Create the dispatcher used to accept connections
         */
        final IEmxDispatcher dispatcher = EmxFactory.getInstance().createDispatcher(EmxFactory.EmxImpl.DEFAULT, "AckReceiverAcceptor", null);

        /*
         * Create the accept event handler
         */
        final AcceptCompleteEventHandler acceptCompleteHandler = new AcceptCompleteEventHandler();

        /*
         * Create the server endpoint
         *
         * Note: Massage descriptor to create a linear link tree (performance)
         */
        ILnkServerEndpoint sep;
        try {
            sep = LnkFactory.getInstance().createServerEndpoint(desc, null);
        }
        catch (Exception e) {
            System.out.println("Failed to create server endpoint <error=" + e.toString() + ">].");
            return;
        }

        /*
         * Accept and spin up a new thread for each accepted link.
         */
        try {
            int connSno = 0;
            while (true) {
                System.out.println("Accepting [desc=" + desc + "]...");
                try {
                    /*
                     * Wait for new link.
                     */
                    sep.acceptPost(dispatcher, acceptCompleteHandler, -1, 0);
                    while (acceptCompleteHandler.eventData == null) {
                        dispatcher.run(-1);
                    }

                    /*
                     * Accept complete.
                     */
                    if (acceptCompleteHandler.eventData.status) {
                        /*
                         * New link. Spin up new thread to handle it.
                         */
                        System.out.println("Accept success.");
                        new Acker(++connSno, acceptCompleteHandler.eventData.pep).start();
                    }
                    else {
                        throw acceptCompleteHandler.eventData.e;
                    }

                    /*
                     * Reset the accept complete handler
                     */
                    acceptCompleteHandler.reset();
                }
                catch (Exception e) {
                    System.out.println("Accept failure [" + e.toString() + "].");
                    break;
                }
            }
        }
        finally {
            try {
                sep.close();
            }
            catch (Exception e) {}
        }
    }

    @Override
    public final void doRun() throws Exception {
        this.desc = desc + "&maxusers=1&dispatchlists=true";
        accept();
    }

    public static void main(String args[]) throws Exception {
        new AckReceiver().run(args);
    }
}
