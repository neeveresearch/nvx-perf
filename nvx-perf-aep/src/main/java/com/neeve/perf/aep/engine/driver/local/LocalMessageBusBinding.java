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
package com.neeve.perf.aep.engine.driver.local;

import java.text.DecimalFormat;

import com.neeve.event.IEventHandler;
import com.neeve.perf.aep.engine.LatencyRecorder;
import com.neeve.perf.serialization.Driver;
import com.neeve.perf.serialization.MessageFactory;
import com.neeve.perf.serialization.Provider;
import com.neeve.perf.serialization.rumi.xbuf2.Car;
import com.neeve.quark.QuarkBuffer;
import com.neeve.quark.QuarkPacket;
import com.neeve.rog.IRogMessage;
import com.neeve.sma.MessageBusDescriptor;
import com.neeve.sma.MessageChannel;
import com.neeve.sma.MessageChannelDescriptor;
import com.neeve.sma.MessageLatencyManager;
import com.neeve.sma.MessageView;
import com.neeve.sma.SmaException;
import com.neeve.sma.impl.MessageBusBindingBase;
import com.neeve.util.UtlGovernor;
import com.neeve.util.UtlThread;

final public class LocalMessageBusBinding extends MessageBusBindingBase implements Runnable {
    final private int _sender = hashCode();
    final private DecimalFormat _dfmt;
    private Provider<?> _provider;
    private QuarkBuffer _serializedMessage;
    private int _serializedMessageLength;
    private String _encoding;
    private int _count;
    private int _warmupTime;
    private int _rate;
    private String _injectorCPUAffinityMask;
    private long _start;
    private boolean _warmupCompleted;
    private int _postWarmupCount;
    private long _postWarmupStart;
    private int _numReceived;

    LocalMessageBusBinding(final String userName,
                           final MessageBusDescriptor descriptor,
                           final IEventHandler eventHandler) throws Exception {
        super(null, userName, descriptor, eventHandler);
        _dfmt = new DecimalFormat("#,###");
    }

    final private void prepareSerializedMessage(final QuarkPacket packet) {
        packet.init(_serializedMessage, 0, _serializedMessageLength);
    }

    final void send(final MessageView view) throws SmaException {
        final long preWireTs = System.nanoTime();

        // this is where the message would be sent out on the outbound transport

        // update stats
        view.setPostWireSendTs(preWireTs);
        view.setPreWireTs(preWireTs);

        // update w2w latency
        try {
            LatencyRecorder.recordW2w(view.getPreWireTs() - view.getPostWireTs());
            if (_warmupCompleted) {
                _postWarmupCount++;
            }
            if (!_warmupCompleted && preWireTs - _start > (_warmupTime * 1000000000L)) {
                System.out.println("Warm up complete.");
                _postWarmupStart = preWireTs;
                _warmupCompleted = true;
            }
            if (++_numReceived == _count) {
                final long stop = System.nanoTime();
                LatencyRecorder.stop();
                final int overallRate = (int)((_postWarmupCount * 1000000000L) / (stop - _postWarmupStart));
                System.out.println("Processed " + _dfmt.format(_postWarmupCount) + " messages @ " + _dfmt.format(overallRate) + " msgs/sec post warmup.");
                System.out.println("Run complete (run rumi-reporter on latencies.*.bin to calculate latency stats)");
            }
        }
        catch (Throwable e) {
            throw new SmaException(e);
        }
    }

    @Override
    final protected void doOpen() throws SmaException {
        _provider = Driver.getProvider(descriptor.getProviderConfig().getProperty("encoding", "xbuf2.serial"));
        _serializedMessage = QuarkBuffer.create(1024, true);
        _serializedMessageLength = (((Car)_provider.create(true)).serializeTo(_serializedMessage, 0));
        _count = Integer.parseInt(descriptor.getProviderConfig().getProperty("count", "10000000"));
        _warmupTime = Integer.parseInt(descriptor.getProviderConfig().getProperty("warmup_time", "2"));
        _rate = Integer.parseInt(descriptor.getProviderConfig().getProperty("rate", "100000"));
        _injectorCPUAffinityMask = descriptor.getProviderConfig().getProperty("injector_cpu_affinity_mask", null);
        if (_injectorCPUAffinityMask != null && _injectorCPUAffinityMask.equalsIgnoreCase("null")) {
            _injectorCPUAffinityMask = null;
        }
        LatencyRecorder.noWrite(Boolean.parseBoolean(descriptor.getProviderConfig().getProperty("lw_nowrite", "false")));
        LatencyRecorder.printIntervalStats(Boolean.parseBoolean(descriptor.getProviderConfig().getProperty("lw_print_interval_stats", "false")));
    }

    @Override
    final protected MessageChannel doGetMessageChannel(final MessageChannelDescriptor descriptor) throws SmaException {
        return new LocalMessageChannel(descriptor, this);
    }

    @Override
    final protected void doStart() throws SmaException {
        new Thread(this).start();
    }

    @Override
    final protected void doFlush(final FlushContext flushContext) throws SmaException {
        if (flushContext != null) {
            switch (flushContext.flushMode) {
                case SYNC_BLOCKING:
                    ((SynchronousBlockingFlushContext)flushContext).complete = true;
                    break;

                case SYNC_NON_BLOCKING:
                    ((SynchronousNonBlockingFlushContext)flushContext).complete = true;
                    break;

                case ASYNC:
                    ((AsynchronousFlushContext)flushContext).syncComplete = true;
                    break;

                default:
                    break;
            }
        }
    }

    @Override
    final protected boolean doCanFail() {
        return false;
    }

    @Override
    final protected boolean doAcksRequireFlush() {
        return false;
    }

    @Override
    final protected void doClose() throws SmaException {
    }

    @Override
    final public void run() {
        try {
            // dump params
            System.out.println("Driver Parameters {");
            System.out.println("...Encoding=" + _provider.name());
            System.out.println("...Count=" + _count);
            System.out.println("...Warmup Time=" + _warmupTime);
            System.out.println("...Rate=" + _rate);
            System.out.println("...Affinity=" + _injectorCPUAffinityMask);

            // affinitize
            if (_injectorCPUAffinityMask != null) {
                UtlThread.setCPUAffinityMask(_injectorCPUAffinityMask);
            }

            // get the channel to dispatch inbound messages on
            final LocalMessageChannel channel = (LocalMessageChannel)getMessageChannel("client");

            // start the latency recorder
            LatencyRecorder.start(_rate, _count);

            // run
            _start = System.nanoTime();
            UtlGovernor.run(_count, _rate, new Runnable() {
                final private QuarkPacket packet = new QuarkPacket();

                @Override
                final public void run() {
                    try {
                        prepareSerializedMessage(packet);
                        final long now = System.nanoTime();
                        LocalMessageBusBinding.this.onMessage(channel,
                                                              LocalMessageBusBinding.this.wrap(packet,
                                                                                               _provider.vfid(),
                                                                                               _provider.otype(),
                                                                                               _provider.encoding(),
                                                                                               _sender,
                                                                                               0,
                                                                                               0l,
                                                                                               null,
                                                                                               0l,
                                                                                               0l,
                                                                                               now,
                                                                                               now),
                                                              null);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
