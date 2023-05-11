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

import java.io.Reader;
import java.text.DecimalFormat;

import com.neeve.emx.EmxFactory;
import com.neeve.emx.IEmxDispatcher;
import com.neeve.link.ILnkClientEndpoint;
import com.neeve.link.ILnkEndpoint;
import com.neeve.link.ILnkEventHandler;
import com.neeve.link.ILnkPeerEndpoint;
import com.neeve.link.ILnkSTRRootEndpoint;
import com.neeve.link.LnkEvents;
import com.neeve.link.LnkFactory;
import com.neeve.link.LnkSender;
import com.neeve.perf.common.LatencyWriter;
import com.neeve.perf.common.SystemProperties;
import com.neeve.pkt.PktFactory;
import com.neeve.pkt.PktPacket;
import com.neeve.pkt.types.PktBodyData;
import com.neeve.pkt.types.PktBodyTypesBase;
import com.neeve.tools.interactive.commands.AnnotatedCommand;
import com.neeve.util.UtlConstants;
import com.neeve.util.UtlGovernor;
import com.neeve.util.UtlThread;

@AnnotatedCommand.Command(keywords = "StreamingLinkSender", description = "A sender for testing ping performance")
final public class PingPongSender extends AnnotatedCommand {
    final private class ConnectCompleteEventHandler implements ILnkEventHandler {
        LnkEvents.ConnectAcceptCompleteEventData eventData;

        @Override
        final public void onEvent(final IEmxDispatcher dispatcher, final ILnkEndpoint ep, final int event, final Object data) {
            this.eventData = (LnkEvents.ConnectAcceptCompleteEventData)data;
        }
    }

    final private class Reader extends Thread {
        final private class EventHandler implements ILnkEventHandler {
            @Override
            final public void onEvent(final IEmxDispatcher dispatcher, final ILnkEndpoint ep, final int event, final Object data) {
                switch (event) {
                    case LnkEvents.EVENT_PACKET:
                        try {
                            final long ts = System.nanoTime();
                            final PktPacket packet = (PktPacket)data;
                            final int latency = (int)((ts - packet.getBody().getBuffer().getLong(0)) / (_oneWay ? 2 : 1));
                            if (_lw != null) {
                                _lw.write(latency);
                            }
                            packet.dispose();
                            _numReceived++;
                            _deltaNumReceived++;
                        }
                        catch (Throwable e) {
                            e.printStackTrace();
                            _done = true;
                        }
                        break;

                    case LnkEvents.EVENT_FAILURE:
                        System.out.println("[PingPongSender] Link (" + ep.toString() + ") failure [" + ((Exception)data).toString() + "]");
                        _done = true;
                        break;

                    default:
                        throw new InternalError("Received event [type=" + event + " data=" + data + "]!");

                }
            }
        }

        final private IEmxDispatcher _dispatcher;
        final private ILnkPeerEndpoint _pep;
        private LatencyWriter _lw;
        private int _numReceived;
        private int _deltaNumReceived;
        private boolean _done;

        Reader(final IEmxDispatcher dispatcher, final ILnkPeerEndpoint pep) {
            _dispatcher = dispatcher;
            _pep = pep;
        }

        final Reader reset() {
            _numReceived = _deltaNumReceived = 0;
            return this;
        }

        final int numReceived() {
            return _numReceived;
        }

        final int deltaNumReceived() {
            return _deltaNumReceived;
        }

        final void clearDeltaNumReceived() {
            _deltaNumReceived = 0;
        }

        final void setLatencyWriter(final LatencyWriter lw) {
            _lw = lw;
        }

        @Override
        final public void run() {
            try {
                // take ownership
                _dispatcher.setOwner();

                // affinitize to CPU
                if (_readerCpuAffinityMask != null) {
                    System.out.println("[PingPongSender] Affinitizing reader thread to CPU " + _readerCpuAffinityMask);
                    UtlThread.setCPUAffinityMask(UtlThread.parseAffinityMask(_readerCpuAffinityMask));
                }

                // join for read
                _pep.join((short)-1, new EventHandler());

                // receive
                System.out.println("[PingPongSender] Receiving packets...");
                ((ILnkSTRRootEndpoint)_pep.getRootEndpoint()).startRead(_dispatcher, 0);
                while (!_done) {
                    _dispatcher.run(-1);
                }
            }
            catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    @Option(shortForm = 'd', longForm = "descriptor", required = true, description = "The connection descriptor to use e.g. tcp://192.168.1.7:12000&tcpnodelay=true")
    private String _descriptor;

    @Option(shortForm = 'm', longForm = "messageSize", defaultValue = "256", required = true, description = "The size of the message to stream")
    private int _messageSize;

    @Option(shortForm = 't', longForm = "testCount", defaultValue = "300000", description = "The test count")
    private int _testCount;

    @Option(shortForm = 'r', longForm = "testRate", defaultValue = "10000", description = "The send rate")
    private int _testRate;

    @Option(shortForm = 'w', longForm = "warmupTime", defaultValue = "2", description = "The warm up time, in seconds")
    private int _warmupTime;

    @Option(shortForm = 'a', longForm = "writerCpuAffinityMask", description = "which CPU(s) to affinitize the writer thread to")
    private String _writerCpuAffinityMask;

    @Option(shortForm = 'b', longForm = "readerCpuAffinityMask", description = "which CPU(s) to affinitize the reader thread to")
    private String _readerCpuAffinityMask;

    @Option(shortForm = 's', longForm = "stats", defaultValue = "false", description = "Whether to output incremental throughput stats")
    private boolean _stats;

    @Option(shortForm = 'o', longForm = "oneWayLatency", description = "whether to calculate one-way latency values")
    private boolean _oneWay;

    @Option(shortForm = 'i', longForm = "printIntervalStats", description = "whether to output stats at periodic intervals instead of only at the end")
    private boolean _printIntervalStats;

    @Option(shortForm = 'f', longForm = "dontWriteLatenciesToFile", description = "whether to suppress writing latency values to a file")
    private boolean _dontWriteLatenciesToFile;

    private void stats(final int numSent,
                       final int deltaNumSent,
                       final int numReceived,
                       final int deltaNumReceived,
                       final long now,
                       final long start,
                       final long deltaStart) {
        final long deltaSendRate = deltaStart > 0 ? ((deltaNumSent * 1000l) / (now - deltaStart)) : 0;
        final long overallSendRate = (numSent * 1000l) / (now - start);
        final long deltaReceiveRate = deltaStart > 0 ? ((deltaNumReceived * 1000l) / (now - deltaStart)) : 0;
        final long overallReceiveRate = (numReceived * 1000l) / (now - start);
        final int pending = numSent - numReceived;
        System.out.println("[PingPongSender] SEND [" + numSent + ","  + deltaSendRate + "," + overallSendRate + "]" + " RECV [" + numReceived + ","  + deltaReceiveRate + "," + overallReceiveRate + "]" + " PENDING [" + pending + "]");
    }

    private void waitForRoundTripsToComplete(final int numSent, final Reader reader) {
        while (numSent > reader.numReceived()) {
            System.out.println("[PingPongSender] Waiting for round trips to complete (sent=" + numSent + ", numRcvd=" + reader.numReceived() + ")...");
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
            }
        }
    }

    final private ILnkPeerEndpoint connect(final IEmxDispatcher dispatcher) throws Exception {
        // create connector
        final ILnkClientEndpoint cep = LnkFactory.getInstance().createClientEndpoint(_descriptor, null);

        // connect
        System.out.println("[PingPongSender] Connecting to " + _descriptor + "]...");
        final ConnectCompleteEventHandler connectCompleteHandler = new ConnectCompleteEventHandler();
        cep.connectPost(dispatcher, connectCompleteHandler, -1, 0);
        while (connectCompleteHandler.eventData == null) {
            dispatcher.run(-1);
        }
        if (connectCompleteHandler.eventData.status) {
            System.out.println("[PingPongSender] Connect success.");
            return connectCompleteHandler.eventData.pep;
        }
        else {
            throw connectCompleteHandler.eventData.e;
        }
    }

    @Override
    public void execute() throws Exception {
        // affinitize to CPU
        if (_writerCpuAffinityMask != null) {
            System.out.println("[PingPongSender] Affinitizing writer thread to CPU " + _writerCpuAffinityMask);
            UtlThread.setCPUAffinityMask(UtlThread.parseAffinityMask(_writerCpuAffinityMask));
        }

        // dump system props
        SystemProperties.dump();

        // dump test parameters
        DecimalFormat dfmt = new DecimalFormat("#,###");
        System.out.println("[PingPongSender] Descriptor:" + _descriptor);
        System.out.println("[PingPongSender] Message size:" + _messageSize);
        System.out.println("[PingPongSender] Test count:" + dfmt.format(_testCount));
        System.out.println("[PingPongSender] Warmup time:" + _warmupTime + "s");
        System.out.println("[PingPongSender] Test rate:" + dfmt.format(_testRate));
        System.out.println("[PingPongSender] Writer CPU affinity mask:" + _writerCpuAffinityMask);
        System.out.println("[PingPongSender] Reader CPU affinity mask:" + _readerCpuAffinityMask);
        System.out.println("[PingPongSender] One Way Latency:" + _oneWay);
        System.out.println("[PingPongSender] Print interval stats:" + _printIntervalStats);
        System.out.println("[PingPongSender] Write latencies to file:" + !_dontWriteLatenciesToFile);

        // create dispatcher
        final IEmxDispatcher dispatcher = EmxFactory.getInstance().createDispatcher(EmxFactory.EmxImpl.DEFAULT, "StreamingReceiverDispatcher", null);

        // establish connection
        final ILnkPeerEndpoint pep = connect(dispatcher);

        // start the reader
        final Reader reader = new Reader(dispatcher, pep);
        reader.start();

        // create the packet to send
        final PktPacket packet = PktFactory.getInstance().createPacket(PktBodyTypesBase.DATA);
        ((PktBodyData)packet.getBody()).setBufferLength(_messageSize);

        // create latency writer
        final LatencyWriter lw = new LatencyWriter("nw-lat", _dontWriteLatenciesToFile ? null : "latencies.send.bin", _printIntervalStats);

        // warm up
        System.out.println("[PingPongSender] Warming up (" + _warmupTime + " seconds)...");
        UtlGovernor governor = new UtlGovernor(_testRate);
        final long warmupStart = System.currentTimeMillis();
        int numSent = 0;
        while ((System.currentTimeMillis() - warmupStart) < (_warmupTime * 1000l)) {
            packet.getBody().getBuffer().putLong(0, System.nanoTime());
            pep.enque((short)-1, packet, null, ILnkPeerEndpoint.IOFLAG_FLUSH_FORCE);
            numSent++; 
            governor.blockToNext();
        }
        waitForRoundTripsToComplete(numSent, reader);

        // run test
        System.out.println("[PingPongSender] Running test (rate=" + dfmt.format(_testRate) + ", count=" + dfmt.format(_testCount) + ")...");
        numSent = 0;
        reader.reset().setLatencyWriter(lw);
        governor = new UtlGovernor(_testRate);
        int deltaNumSent = 0;
        final long start = System.currentTimeMillis();
        long deltaStart = start;
        lw.start(_testRate, _testCount);
        for (int i = 0 ; i < _testCount ; i++) {
            packet.getBody().getBuffer().putLong(0, System.nanoTime());
            pep.enque((short)-1, packet, null, ILnkPeerEndpoint.IOFLAG_FLUSH_FORCE);
            numSent++;
            if (_stats) {
                deltaNumSent++;
                final long now = System.currentTimeMillis();
                if (now - deltaStart >= 1000l) {
                    stats(numSent, deltaNumSent, reader.numReceived(), reader.deltaNumReceived(), now, start, deltaStart);
                    deltaStart = now;
                    deltaNumSent = 0;
                    reader.clearDeltaNumReceived();
                }
            }
            governor.blockToNext();
        }
        final long stop = System.currentTimeMillis();
        waitForRoundTripsToComplete(numSent, reader);
        lw.stop();
        System.out.println("[PingPongSender] Test complete.");
        if (_stats) stats(numSent, deltaNumSent, reader.numReceived(), reader.deltaNumReceived(), stop, start, 0);

        // close connection
        pep.close((short)-1);

        // done
        if (!_dontWriteLatenciesToFile) {
            System.out.println("[PingPongSender] Test complete (run rumi-reporter on latencies.send.bin to calculate latency stats).");
        }
        else {
            System.out.println("[PingPongSender] Test complete.");
        }
    }

    public static void main(String args[]) throws Exception {
        System.setProperty(UtlConstants.THREAD_ENABLECPUAFFINITYMASKS_PROPNAME, "true");
        new PingPongSender().run(args);
    }
}
