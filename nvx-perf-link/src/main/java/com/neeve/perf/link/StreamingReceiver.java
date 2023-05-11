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
import com.neeve.pkt.PktPacket;
import com.neeve.tools.interactive.commands.AnnotatedCommand;

/**
 * Receiver in the link performance test package.
 */
@AnnotatedCommand.Command(keywords = "StreamingReceiver", description = "Starts a receiver for testing streaming network performance")
final public class StreamingReceiver extends Common {
    /*
     * Accept complete event handler
     */
    final private class AcceptCompleteEventHandler implements ILnkEventHandler {
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
     * Link event handler
     */
    final private class EventHandler implements ILnkEventHandler {
        /**
         * Implementation of {@link ILnkEventHandler#onEvent}
         */
        final public void onEvent(final IEmxDispatcher dispatcher,
                                  final ILnkEndpoint ep,
                                  final int event,
                                  final Object data) {
            if (event == LnkEvents.EVENT_PACKET) {
                final PktPacket packet = (PktPacket)data;
                if (!packetManager.onReceive(packet)) {
                    packet.dispose();
                }
            }
            else if (event == LnkEvents.EVENT_FAILURE) {
                System.out.println("Failure [" + ((Exception)data).toString() + "]");
                done = true;
            }
        }
    }

    /*
     * Private members
     */
    @Argument(position = 1, name = "descriptor", required = true, defaultValue = "tcp://localhost:10000", description = "The server descriptor to use.")
    private String desc;
    private IEmxDispatcher dispatcher;
    private AcceptCompleteEventHandler acceptCompleteHandler;
    private ILnkEventHandler eventHandler;
    private ILnkPeerEndpoint pep;
    private boolean done;

    /*
     * Constructor
     */
    public StreamingReceiver() throws Exception {
        super();
    }

    final private boolean accept() {
        System.out.println("Accepting [desc=" + desc + "]...");

        /*
         * Create the server endpoint
         */
        ILnkServerEndpoint sep;
        try {
            sep = LnkFactory.getInstance().createServerEndpoint(desc, null);
        }
        catch (Exception e) {
            System.out.println("Accept failure [Failed to create server endpoint <error=" + e.toString() + ">].");
            return false;
        }

        /*
         * Accept
         */
        try {
            try {
                sep.acceptPost(dispatcher, acceptCompleteHandler, -1, 0);
                while (acceptCompleteHandler.eventData == null) {
                    dispatcher.run(-1);
                }
                if (acceptCompleteHandler.eventData.status) {
                    System.out.println("Accept success.");
                    this.pep = acceptCompleteHandler.eventData.pep;
                }
                else {
                    throw acceptCompleteHandler.eventData.e;
                }
            }
            catch (Exception e) {
                System.out.println("Accept failure [" + e.toString() + "].");
                return false;
            }

            /*
             * Join
             */
            try {
                pep.join((short)-1, eventHandler);
                return true;
            }
            catch (Exception e) {
                System.out.println("Join failure [" + e.toString() + "].");
                try {
                    pep.close((short)-1);
                }
                catch (Exception e1) {}
                return false;
            }
        }
        finally {
            try {
                sep.close();
            }
            catch (Exception e) {}
        }
    }

    final private void receive() {
        try {
            /*
             * Start the read machinery
             */
            try {
                System.out.println("Starting read machinery...");
                ((ILnkSTRRootEndpoint)pep.getRootEndpoint()).startRead(dispatcher, 0);
            }
            catch (Exception e) {
                System.out.println("Read machinery start failure [" + e.toString() + "].");
                return;
            }

            /*
             * Dispatch
             */
            System.out.println("Receiving packets...");
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

    @Override
    final public void doRun() throws Exception {
        this.dispatcher = EmxFactory.getInstance().createDispatcher(EmxFactory.EmxImpl.DEFAULT, "StreamingReceiverDispatcher", null);
        this.acceptCompleteHandler = new AcceptCompleteEventHandler();
        this.eventHandler = new EventHandler();
        if (accept()) {
            receive();
        }
    }

    public static void main(String args[]) throws Exception {
        new StreamingReceiver().run(args);
    }
}
