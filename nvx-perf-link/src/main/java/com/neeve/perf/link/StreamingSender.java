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

import java.text.DecimalFormat;

import com.neeve.emx.EmxFactory;
import com.neeve.emx.IEmxDispatcher;
import com.neeve.link.ILnkClientEndpoint;
import com.neeve.link.ILnkEndpoint;
import com.neeve.link.ILnkEventHandler;
import com.neeve.link.ILnkPeerEndpoint;
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
import com.neeve.util.UtlThread;

@AnnotatedCommand.Command(keywords = "StreamingLinkSender", description = "A sender for testing streaming performance")
final public class StreamingSender extends AnnotatedCommand {
    final private class ConnectCompleteEventHandler implements ILnkEventHandler {
        LnkEvents.ConnectAcceptCompleteEventData eventData;

        @Override
        final public void onEvent(final IEmxDispatcher dispatcher, final ILnkEndpoint ep, final int event, final Object data) {
            this.eventData = (LnkEvents.ConnectAcceptCompleteEventData)data;
        }
    }

    final private class EventHandler implements ILnkEventHandler {
        @Override
        final public void onEvent(final IEmxDispatcher dispatcher, final ILnkEndpoint ep, final int event, final Object data) {
            switch (event) {
                case LnkEvents.EVENT_FAILURE:
                    System.out.println("[StreamingSender] Link (" + ep.toString() + ") failure [" + ((Exception)data).toString() + "]");
                    break;

                default:
                    throw new InternalError("Received event [type=" + event + " data=" + data + "]!");

            }
        }
    }

    @Option(shortForm = 'd', longForm = "descriptor", required = true, description = "The connection descriptor to use e.g. tcp://192.168.1.7:12000&tcpnodelay=true")
    private String _descriptor;

    @Option(shortForm = 'm', longForm = "messageSize", defaultValue = "256", required = true, description = "The size of the message to stream")
    private int _messageSize;

    @Option(shortForm = 'a', longForm = "flushAfter", defaultValue = "64", description = "Flush after how many messages")
    private int _flushAfter;

    @Option(shortForm = 't', longForm = "testCount", defaultValue = "100000000", description = "The test count")
    private int _testCount;

    @Option(shortForm = 'r', longForm = "testRate", defaultValue = "10000000", description = "The send rate")
    private int _testRate;

    @Option(shortForm = 'w', longForm = "warmupTime", defaultValue = "2", description = "The warm up time, in seconds")
    private int _warmupTime;

    @Option(shortForm = 'c', longForm = "cpuAffinityMask", description = "which CPU(s) to affinitize the sending thread to")
    private String _cpuAffinityMask;

    @Option(shortForm = 'i', longForm = "printIntervalStats", description = "whether to output stats at periodic intervals instead of only at the end")
    private boolean _printIntervalStats;

    @Option(shortForm = 'f', longForm = "dontWriteLatenciesToFile", description = "whether to suppress writing latency values to a file")
    private boolean _dontWriteLatenciesToFile;

    final private ILnkPeerEndpoint connect(final IEmxDispatcher dispatcher) throws Exception {
        // create connector
        final ILnkClientEndpoint cep = LnkFactory.getInstance().createClientEndpoint(_descriptor, null);

        // connect
        System.out.println("[StreamingSender] Connecting to " + _descriptor + "]...");
        final ConnectCompleteEventHandler connectCompleteHandler = new ConnectCompleteEventHandler();
        cep.connectPost(dispatcher, connectCompleteHandler, -1, 0);
        while (connectCompleteHandler.eventData == null) {
            dispatcher.run(-1);
        }
        if (connectCompleteHandler.eventData.status) {
            System.out.println("[StreamingSender] Connect success.");
            return connectCompleteHandler.eventData.pep;
        }
        else {
            throw connectCompleteHandler.eventData.e;
        }
    }

    @Override
    public void execute() throws Exception {
        // affinitize to CPU
        if (_cpuAffinityMask != null) {
            System.out.println("[StreamingSender] Affinitizing thread to CPU " + _cpuAffinityMask);
            UtlThread.setCPUAffinityMask(UtlThread.parseAffinityMask(_cpuAffinityMask));
        }

        // dump system props
        SystemProperties.dump();

        // dump test parameters
        DecimalFormat dfmt = new DecimalFormat("#,###");
        System.out.println("[StreamingSender] Descriptor:" + _descriptor);
        System.out.println("[StreamingSender] Message size:" + _messageSize);
        System.out.println("[StreamingSender] Test count:" + dfmt.format(_testCount));
        System.out.println("[StreamingSender] Warmup time:" + _warmupTime + "s");
        System.out.println("[StreamingSender] Test rate:" + dfmt.format(_testRate));
        System.out.println("[StreamingSender] CPU affinity mask:" + _cpuAffinityMask);
        System.out.println("[StreamingSender] Print interval stats:" + _printIntervalStats);
        System.out.println("[StreamingSender] Write latencies to file:" + !_dontWriteLatenciesToFile);

        // create dispatcher
        final IEmxDispatcher dispatcher = EmxFactory.getInstance().createDispatcher(EmxFactory.EmxImpl.DEFAULT, "StreamingReceiverDispatcher", null);

        // establish connection
        final ILnkPeerEndpoint pep = connect(dispatcher);
        pep.join((short)-1, new EventHandler());

        // calculate nanoTime() overhead
        System.out.println("[StreamingSender] Calculating nanoTime() overhead...");
        long nanoTimeOverhead = 0l;
        long start = System.nanoTime();
        for (int i = 0; i < 100000000l; i++) {
            System.nanoTime();
        }
        nanoTimeOverhead = (System.nanoTime() - start) / 100000000l;
        System.out.println("[StreamingSender] ..." + nanoTimeOverhead + "ns");

        // create latency writer
        final LatencyWriter lw = new LatencyWriter("nw-write", _dontWriteLatenciesToFile ? null : "latencies.write.bin", _printIntervalStats);

        // create the packet to send
        final PktPacket packet = PktFactory.getInstance().createPacket(PktBodyTypesBase.DATA);
        ((PktBodyData)packet.getBody()).setBufferLength(_messageSize);

        // warm up
        System.out.println("[StreamingSender] Warming up (" + _warmupTime + " seconds)...");
        final long warmupStart = System.currentTimeMillis();
        while ((System.currentTimeMillis() - warmupStart) < (_warmupTime * 1000l)) {
            pep.enque((short)-1, packet, null, 0);
        }
        pep.flush((short)-1, null);

        // run test
        int numSent = 0;
        long ts1, ts2;
        final long nanosPerMsg = _testRate > 0 ? (1000000000l / _testRate) : 0;
        System.out.println("[StreamingSender] Running test (" + dfmt.format(_testCount) + " messages @ " + _testRate + " msgs/sec)...");
        long next = System.nanoTime() + nanosPerMsg;
        lw.start(_testRate, _testCount);
        start = System.currentTimeMillis();
        while (numSent < _testCount) {
            ts1 = System.nanoTime();
            if (ts1 >= next) {
                pep.enque((short)-1, packet, null, 0);
                numSent++;
                if (numSent % _flushAfter == 0) {
                    pep.flush((short)-1, null);
                }
                ts2 = System.nanoTime();
                final int writeTime = (int)(ts2 - ts1 - nanoTimeOverhead);
                lw.write(writeTime);
                next += nanosPerMsg;
            }
        }
        final long stop = System.currentTimeMillis();
        lw.stop();

        // close link
        pep.close((short)-1);

        // stats
        final long overallRate = (numSent * 1000l) / (stop - start);
        System.out.println("[StreamingSender] Sent " + dfmt.format(numSent) + " messages at " + dfmt.format(overallRate) + " msgs/sec.");
        if (!_dontWriteLatenciesToFile) {
            System.out.println("[StreamingSender] Test complete (run rumi-reporter on latencies.write.bin to calculate latency stats).");
        }
        else {
            System.out.println("[StreamingSender] Test complete.");
        }
    }

    public static void main(String args[]) throws Exception {
        System.setProperty(UtlConstants.THREAD_ENABLECPUAFFINITYMASKS_PROPNAME, "true");
        new StreamingSender().run(args);
    }
}
