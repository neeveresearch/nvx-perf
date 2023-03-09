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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.neeve.config.Config;
import com.neeve.event.IEventHandler;
import com.neeve.io.IONativePacket;
import com.neeve.io.IOBuffer;
import com.neeve.perf.aep.engine.LatencyRecorder;
import com.neeve.perf.aep.messages.Message;
import com.neeve.perf.aep.messages.MessageFactory;
import com.neeve.sma.MessageBusDescriptor;
import com.neeve.sma.MessageChannel;
import com.neeve.sma.MessageChannelDescriptor;
import com.neeve.sma.MessageLatencyManager;
import com.neeve.sma.MessageView;
import com.neeve.sma.SmaException;
import com.neeve.sma.impl.MessageBusBindingBase;
import com.neeve.util.UtlGovernor;
import com.neeve.util.UtlThread;
import com.neeve.util.UtlTime;

final public class LocalMessageBusBinding extends MessageBusBindingBase implements Runnable {
    final private int sendCount = (int)Config.getValue("driver.sendCount", 10000);
    final private int sendRate = (int)Config.getValue("driver.sendRate", 1000);
    final private long sendAffinity = UtlThread.parseAffinityMask(Config.getValue("driver.sendAffinity", "0"));
    final private int sender = hashCode();
    final private long inBufferAddress;
    final private Message.Serializer messageSerializer;
    private int totalReceived;

    LocalMessageBusBinding(final String userName,
                           final MessageBusDescriptor descriptor,
                           final IEventHandler eventHandler) throws Exception {
        super(null, userName, descriptor, eventHandler);
        inBufferAddress = IOBuffer.allocateMemoryBlock(1024, false);
        messageSerializer = Message.Serializer.create();
    }

    final private void prepareSerializedMessage(final IONativePacket packet) {
        messageSerializer.init(inBufferAddress, 0).key("test");
        packet.init(inBufferAddress, 0, messageSerializer.done());
    }

    final void send(final MessageView view) throws SmaException {
        final long preWireTs = UtlTime.now();

        // this is where the message would be sent out on the outbound transport

        // update stats
        view.setPostWireSendTs(preWireTs);
        view.setPreWireTs(preWireTs);
        if (MessageLatencyManager.captureMsgLatencyStats && latencyManager != null) latencyManager.update(view, MessageLatencyManager.MessagingDirection.Outbound);

        // update w2w latency
        try {
            LatencyRecorder.recordW2w(view.getPreWireTs() - view.getPostWireTs());
            if (++totalReceived == sendCount) {
                try {
                    LatencyRecorder.stop();
                    System.out.println("Done");
                }
                catch (Exception ex_) {
                    ex_.printStackTrace();
                }
                finally {
                    System.exit(0);
                }
            }
        }
        catch (Throwable e) {
            throw new SmaException(e);
        }
    }

    @Override
    final protected void doOpen() throws SmaException {
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
    final protected void doFlush() throws SmaException {
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
            System.out.println("*** Send Rate=" + sendRate);
            System.out.println("*** Send Count=" + sendCount);
            System.out.println("*** Send Affinity=" + sendAffinity);
            UtlThread.setCPUAffinityMask(sendAffinity);

            final LocalMessageChannel channel = (LocalMessageChannel)getMessageChannel("client");
            LatencyRecorder.start(sendRate, sendCount);
            UtlGovernor.run(sendCount, sendRate, new Runnable() {
                final private IONativePacket packet = new IONativePacket();

                @Override
                final public void run() {
                    try {
                        prepareSerializedMessage(packet);
                        final long now = UtlTime.now();
                        LocalMessageBusBinding.this.onMessage(channel,
                                                              LocalMessageBusBinding.this.wrap(packet,
                                                                                               MessageFactory.VFID,
                                                                                               MessageFactory.ID_Message,
                                                                                               MessageView.ENCODING_TYPE_QUARK,
                                                                                               sender,
                                                                                               0,
                                                                                               0l,
                                                                                               0l,
                                                                                               null,
                                                                                               null,
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
