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

import com.neeve.event.Event;
import com.neeve.perf.common.LatencyWriter;
import com.neeve.perf.common.SystemProperties;
import com.neeve.sma.MessageView;
import com.neeve.sma.event.MessageEvent;
import com.neeve.sma.event.SmaEventTypes;
import com.neeve.tools.interactive.commands.AnnotatedCommand;
import com.neeve.util.UtlConstants;
import com.neeve.util.UtlTime;

@AnnotatedCommand.Command(keywords = "Receiver", description = "A receiver to benchmark SMA Performance")
final public class Receiver extends Common {
    private LatencyWriter _lw;
    private int _numReceived;
    private boolean _done;

    @Override
    final public void onEvent(final Event event) {
        switch (event.getType()) {
            case SmaEventTypes.MESSAGE:
                try {
                    final MessageView message = ((MessageEvent)event).getMessageView();
                    if (_numReceived == 0) {
                        _lw.start(_testRate, _testCount);
                    }
                    final int latency = (int)(UtlTime.nowSinceEpoch() - message.getOriginTs());
                    _lw.write(latency * 1000);
                    if (++_numReceived == _testCount) {
                        _lw.stop();
                        _done = true;
                    }
                    break;
                }
                catch (Throwable e) {
                    e.printStackTrace();
                    _done = true;
                }

            default:
                System.out.println("Received unprocessed event [" + event + "]");

        }
    }

    @Override
    final public void execute() throws Exception {
        // dump system props
        SystemProperties.dump();

        DecimalFormat dfmt = new DecimalFormat("#,###");
        System.out.println("[Receiver] Bus Descriptor......" + _descriptor);
        System.out.println("[Receiver] Test Count.........." + dfmt.format(_testCount));
        System.out.println("[Receiver] Message Encoding...." + _encoding);
        System.out.println("[Receiver] Channel Key........." + _channelKey);
        System.out.println("[Receiver] Channel Filter......" + _channelFilter);
        System.out.println("[Receiver] Channel Qos........." + _channelQos);

        // connect to bus
        connect(true);

        // create latency writer
        _lw = new LatencyWriter("nw-lat", _dontWriteLatenciesToFile ? null : "latencies.send.bin", _printIntervalStats);

        // wait until done
        while (!_done) {
            Thread.sleep(100);
        }

        // close bus connection
        _binding.close();
    }

    public static void main(String[] args) throws Exception {
        System.setProperty(UtlConstants.THREAD_ENABLECPUAFFINITYMASKS_PROPNAME, "true");
        new Receiver().run(args);
    }
}
