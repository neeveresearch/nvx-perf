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
import com.neeve.perf.common.SystemProperties;
import com.neeve.pkt.PktFactory;
import com.neeve.pkt.PktPacket;
import com.neeve.pkt.types.PktBodyData;
import com.neeve.pkt.types.PktBodyTypesBase;
import com.neeve.tools.interactive.commands.AnnotatedCommand;
import com.neeve.util.UtlConstants;
import com.neeve.util.UtlThread;

@AnnotatedCommand.Command(keywords = "PingPongReceiver", description = "A receiver for testing streaming performance")
final public class PingPongReceiver extends AnnotatedCommand {
    final private class AcceptCompleteEventHandler implements ILnkEventHandler {
        LnkEvents.ConnectAcceptCompleteEventData eventData;

        @Override
        final public void onEvent(final IEmxDispatcher dispatcher, final ILnkEndpoint ep, final int event, final Object data) {
            this.eventData = (LnkEvents.ConnectAcceptCompleteEventData)data;
        }
    }

    final private class EventHandler implements ILnkEventHandler {
        final private PktPacket sendPacket;
        private long start;
        private long deltaStart;
        private int numRcvd;
        private int deltaNumRcvd;

        EventHandler() {
            sendPacket = PktFactory.getInstance().createPacket(PktBodyTypesBase.DATA);
            ((PktBodyData)sendPacket.getBody()).setBufferLength(8);
        }

        @Override
        final public void onEvent(final IEmxDispatcher dispatcher, final ILnkEndpoint ep, final int event, final Object data) {
            switch (event) {
                case LnkEvents.EVENT_PACKET:
                    // process
                    try {
                        final PktPacket receivedPacket = ((PktPacket)data);
                        final long ts = receivedPacket.getBody().getBuffer().getLong(0);
                        sendPacket.getBody().getBuffer().putLong(0, ts);
                        receivedPacket.dispose();
                        ((ILnkPeerEndpoint)ep).enque((short)-1, sendPacket, null, ILnkPeerEndpoint.IOFLAG_FLUSH_FORCE);
                    }
                    catch (Throwable e) {
                        e.printStackTrace();
                        _done = true;
                    }

                    // stats
                    if (!_done && _stats) {
                        final long now = System.currentTimeMillis(); 
                        if (start == 0l) {
                            start = deltaStart = now;
                        }
                        numRcvd++;
                        deltaNumRcvd++;
                        if (now - deltaStart >= 1000l) {
                            final long deltaRate = (deltaNumRcvd * 1000l) / (now - deltaStart);
                            final long overallRate = (numRcvd * 1000l) / (now - start);
                            System.out.println("[PingPongReceiver] RATE [" + deltaRate + "," + overallRate + "]");
                            deltaStart = now;
                            deltaNumRcvd = 0;
                        }
                    }
                    break;

                case LnkEvents.EVENT_FAILURE:
                    System.out.println("[PingPongReceiver] Failure [" + ((Exception)data).toString() + "]");
                    _done = true;
                    break;

                default:
                    break;

            }
        }
    }

    @Option(shortForm = 'd', longForm = "descriptor", required = true, description = "The connection descriptor to use e.g. tcp://192.168.1.7:12000&tcpnodelay=true")
    private String _descriptor;

    @Option(shortForm = 'c', longForm = "cpuAffinityMask", description = "the CPU() to affinitize the reading thread to")
    private String _cpuAffinityMask;

    @Option(shortForm = 's', longForm = "stats", defaultValue = "false", required = true, description = "Whether to output incremental throughput stats")
    private boolean _stats;

    private boolean _done;

    final private ILnkPeerEndpoint accept(final IEmxDispatcher dispatcher) throws Exception {
        // create acceptor
        final ILnkServerEndpoint sep = LnkFactory.getInstance().createServerEndpoint(_descriptor, null);

        // block and wait for inbound connection
        try {
            System.out.println("[PingPongReceiver] Accepting [_descriptor=" + _descriptor + "]...");
            final AcceptCompleteEventHandler acceptCompleteHandler = new AcceptCompleteEventHandler();
            sep.acceptPost(dispatcher, acceptCompleteHandler, -1, 0);
            while (acceptCompleteHandler.eventData == null) {
                dispatcher.run(-1);
            }
            if (acceptCompleteHandler.eventData.status) {
                System.out.println("[PingPongReceiver] Accept success.");
                return acceptCompleteHandler.eventData.pep;
            }
            else {
                throw acceptCompleteHandler.eventData.e;
            }
        }
        finally {
            sep.close(); 
        }
    }

    final private void receive(final IEmxDispatcher dispatcher, final ILnkPeerEndpoint pep) throws Exception {
        try {
            // join the link to receive data
            pep.join((short)-1, new EventHandler());

            // receive
            System.out.println("[PingPongReceiver] Receiving packets...");
            ((ILnkSTRRootEndpoint)pep.getRootEndpoint()).startRead(dispatcher, 0);
            while (!_done) {
                dispatcher.run(-1);
            }
        }
        finally {
            pep.close((short)-1);
        }
    }

    @Override
    public void execute() throws Exception {
        // affinitize to CPU
        if (_cpuAffinityMask != null) {
            System.out.println("[PingPongReceiver] Affinitizing thread to CPU " + _cpuAffinityMask);
            UtlThread.setCPUAffinityMask(UtlThread.parseAffinityMask(_cpuAffinityMask));
        }

        // dump system props
        SystemProperties.dump();

        // create dispatcher
        final IEmxDispatcher dispatcher = EmxFactory.getInstance().createDispatcher(EmxFactory.EmxImpl.DEFAULT, "PingPongReceiverDispatcher", null);

        // accept connection
        final ILnkPeerEndpoint pep = accept(dispatcher);

        // receive data
        receive(dispatcher, pep);
    }

    public static void main(String args[]) throws Exception {
        System.setProperty(UtlConstants.THREAD_ENABLECPUAFFINITYMASKS_PROPNAME, "true");
        new PingPongReceiver().run(args);
    }
}
