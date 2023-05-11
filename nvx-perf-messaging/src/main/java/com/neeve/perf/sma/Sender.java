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
package com.neeve.perf.sma;

import java.text.DecimalFormat;

import com.neeve.ci.XRuntime;
import com.neeve.event.Event;
import com.neeve.event.IEventHandler;
import com.neeve.perf.common.LatencyWriter;
import com.neeve.perf.common.SystemProperties;
import com.neeve.perf.serialization.CarFactory;
import com.neeve.sma.MessageChannel;
import com.neeve.sma.MessageView;
import com.neeve.sma.SmaException;
import com.neeve.tools.interactive.commands.AnnotatedCommand;
import com.neeve.util.UtlConstants;
import com.neeve.util.UtlThread;
import com.neeve.util.UtlTime;

@AnnotatedCommand.Command(keywords = "Sender", description = "A sender to benchmark SMA Performance")
final public class Sender extends Common implements IEventHandler {
    @Option(shortForm = 'm', longForm = "messageSize", required = false, defaultValue = "256", description = "the message data size.")
    private int _messageSize;

    @Option(shortForm = 'p', longForm = "dontPopulateMessage", description = "whether to not populate outbound messages with full content i.e. only timestamp is sent")
    boolean _dontPopulate;

    @Option(shortForm = 'a', longForm = "cpuAffinityMask", description = "which CPU(s) to affinitize the sending thread to")
    private String _cpuAffinityMask;

    final private void sendMessage(final MessageView message, final MessageChannel channel) throws SmaException {
        _channel.sendMessage(message, null, MessageChannel.ALREADY_SYNCD | MessageChannel.KEY_ALREADY_RESOLVED | MessageChannel.KEY_ALREADY_VALIDATED );
    }

    @Override
    final public void onEvent(final Event event) {}

    @Override
    final public void execute() throws Exception {
        // affinitize to CPU
        if (_cpuAffinityMask != null) {
            System.out.println("[Sender] Affinitizing thread to CPU " + _cpuAffinityMask);
            UtlThread.setCPUAffinityMask(UtlThread.parseAffinityMask(_cpuAffinityMask));
        }

        // dump system props
        SystemProperties.dump();

        // dump test parameters
        DecimalFormat dfmt = new DecimalFormat("#,###");
        System.out.println("[Sender] Bus Descriptor......" + _descriptor);
        System.out.println("[Sender] Test Count.........." + dfmt.format(_testCount));
        System.out.println("[Sender] Test Rate..........." + dfmt.format(_testRate));
        System.out.println("[Sender] Message Size........" + _messageSize);
        System.out.println("[Sender] Message Encoding...." + _encoding);
        System.out.println("[Sender] Populate Message...." + !_dontPopulate);
        System.out.println("[Sender] Channel Key........." + _channelKey);
        System.out.println("[Sender] Channel Qos........." + _channelQos);
        System.out.println("[Sender] CPU affinity mask..." + _cpuAffinityMask);

        // connect to bus
        connect(false);

        // create the message
        final MessageView message = new CarFactory(_encoding).createCar(!_dontPopulate);

        // calculate UtlTime.now() overhead
        System.out.println("[Sender] Calculating UtlTime.now() overhead...");
        long nanoTimeOverhead = 0l;
        long start = System.nanoTime();
        for (int i = 0; i < 100000000l; i++) {
            UtlTime.now();
        }
        nanoTimeOverhead = (System.nanoTime() - start) / 100000000l;
        System.out.println("[Sender] ..." + nanoTimeOverhead + "ns");

        // create latency writer
        final LatencyWriter lw = new LatencyWriter("nw-write", _dontWriteLatenciesToFile ? null : "latencies.write.bin", _printIntervalStats);

        // run test
        int numSent = 0;
        long ts1, ts2, ts3;
        final long nanosPerMsg = _testRate > 0 ? (1000000000l / _testRate) : 0;
        System.out.println("[Sender] Running test (" + dfmt.format(_testCount) + " messages @ " + _testRate + " msgs/sec)...");
        long next = System.nanoTime() + nanosPerMsg;
        lw.start(_testRate, _testCount);
        start = System.currentTimeMillis();
        while (numSent < _testCount) {
            ts1 = System.nanoTime();
            if (ts1 >= next) {
                ts3 = UtlTime.now();
                message.setOriginTs(UtlTime.nowSinceEpoch());
                sendMessage(message, _channel);
                numSent++;
                ts2 = message.getPreWireTs();
                final int writeTime = (int)((ts2 - ts3) * 1000 - nanoTimeOverhead);
                lw.write(writeTime);
                next += nanosPerMsg;
            }
        }
        final long stop = System.currentTimeMillis();
        lw.stop();

        // close bus connection
        _binding.close();

        // stats
        final long overallRate = (numSent * 1000l) / (stop - start);
        System.out.println("[Sender] Sent " + dfmt.format(numSent) + " messages at " + dfmt.format(overallRate) + " msgs/sec.");
        if (!_dontWriteLatenciesToFile) {
            System.out.println("[Sender] Test complete (run rumi-reporter on latencies.write.bin to calculate latency stats).");
        }
        else {
            System.out.println("[Sender] Test complete.");
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty(UtlConstants.THREAD_ENABLECPUAFFINITYMASKS_PROPNAME, "true");
        new Sender().run(args);
    }
}
