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
package com.neeve.perf.link.link;

import com.neeve.emx.EmxFactory;
import com.neeve.emx.IEmxDispatcher;
import com.neeve.link.ILnkClientEndpoint;
import com.neeve.link.ILnkEndpoint;
import com.neeve.link.ILnkEventHandler;
import com.neeve.link.ILnkPeerEndpoint;
import com.neeve.link.LnkEvents;
import com.neeve.link.LnkFactory;
import com.neeve.link.LnkSender;
import com.neeve.pkt.PktPacket;
import com.neeve.tools.interactive.commands.AnnotatedCommand;

/**
 * Sender in the link performance test package.
 */
@AnnotatedCommand.Command(keywords = "StreamingLinkSender", description = "Starts a client for testing streaming network performance")
final public class StreamingSender extends Common {
    /*
     * Connect complete event handler
     */
    final private class ConnectCompleteEventHandler implements ILnkEventHandler {
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
            if (event == LnkEvents.EVENT_FAILURE) {
                System.out.println("Link (" + ep.toString() + ") failure [" + ((Exception)data).toString() + "]");
            }
            else {
                throw new InternalError("Received event [type=" + event + " data=" + data + "] through performance sender event handler!");
            }
        }
    }

    /*
     * Private members
     */
    @RemainingArgs(name = "descriptors", required = true, description = "A space separated set of sender descriptors to create")
    String[] descs;
    private IEmxDispatcher dispatcher;
    private ILnkPeerEndpoint[] peps;
    private LnkSender[] senders;

    /** 
     * Constructor
     */
    public StreamingSender() throws Exception {
        super();
    }

    final private boolean connect() {
        /*
         * Connect each of the links.
         */
        for (int i = 0; i < descs.length; i++) {
            /*
             * Get next link descriptor 
             */
            final String desc = descs[i];

            /*
             * Create the client endpoint
             */
            ILnkClientEndpoint cep;
            try {
                cep = LnkFactory.getInstance().createClientEndpoint(desc, null);
            }
            catch (Exception e) {
                System.out.println("Connect failure: Failed to create client endpoint [error=" + e.toString() + "].");
                return false;
            }

            /*
             * Connect
             */
            System.out.println("Connecting [desc=" + desc + "]...");
            final ConnectCompleteEventHandler connectCompleteHandler = new ConnectCompleteEventHandler();
            try {
                cep.connectPost(dispatcher, connectCompleteHandler, -1, 0);
                while (connectCompleteHandler.eventData == null) {
                    dispatcher.run(-1);
                }
                if (connectCompleteHandler.eventData.status) {
                    System.out.println("Connect success.");
                    peps[i] = connectCompleteHandler.eventData.pep;
                }
                else {
                    throw connectCompleteHandler.eventData.e;
                }
            }
            catch (Exception e) {
                System.out.println("Connect failure [" + e.toString() + "].");
                try {
                    cep.close();
                }
                catch (Exception e1) {}
                return false;
            }

            /*
             * Join
             */
            System.out.println("Joining...");
            try {
                peps[i].join((short)-1, new EventHandler());
            }
            catch (Exception e) {
                System.out.println("Join failure [" + e.toString() + "].");
                try {
                    peps[i].close((short)-1);
                }
                catch (Exception e1) {}
                return false;
            }

            /*
             * Create/open senders
             */
            System.out.println("Creating senders...");
            try {
                (senders[i] = LnkSender.create(peps[i])).open();
            }
            catch (Exception e) {
                System.out.println("Sender create/open failure [" + e.toString() + "].");
                try {
                    peps[i].close((short)-1);
                }
                catch (Exception e1) {}
                return false;
            }
        }
        return true;
    }

    final private void send() {

        try {
            System.out.println("Sending packets...");
            final long start = System.nanoTime();
            final long nanosPerPacket = rate > 0 ? (1000000000l / rate) : 0;
            long next = start + nanosPerPacket;
            if (count <= 0) count = Long.MAX_VALUE;
            for (int j = 0 ; j < count ; j++) {
                final long current = System.nanoTime();
                if (current >= next) {
                    try {
                        for (int i = 0; i < peps.length; i++) {
                            final PktPacket packet = packetManager.getPacketForSend();
                            senders[i].sendStreaming(prepHeaders(packet), null, 0);
                            packet.dispose();
                        }
                    }
                    catch (Exception e) {
                        System.out.println("Send failure [" + e.toString() + "].");
                        e.printStackTrace();
                        break;
                    }
                    next += nanosPerPacket;
                }
            }
        }
        finally {
            try {
                for (int i = 0; i < peps.length; i++) {
                    peps[i].close((short)-1);
                }
            }
            catch (Exception e1) {}
        }
    }

    @Override
    final public void doRun() throws Exception {
        this.dispatcher = EmxFactory.getInstance().createDispatcher(EmxFactory.EmxImpl.DEFAULT, "LinkPerfSenderDispatcher", null);
        this.peps = new ILnkPeerEndpoint[descs.length];
        this.senders = new LnkSender[descs.length];
        if (connect()) {
            send();
        }
    }

    public static void main(String args[]) throws Exception {
        new StreamingSender().run(args);
    }
}
