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

import com.neeve.config.ConfigRepositoryFactory;
import com.neeve.event.IEventHandler;
import com.neeve.sma.MessageBusBinding;
import com.neeve.sma.MessageBusBindingFactory;
import com.neeve.sma.MessageBusDescriptor;
import com.neeve.sma.MessageChannel;
import com.neeve.sma.MessageChannelDescriptor;
import com.neeve.sma.MessageViewFactoryRegistry;
import com.neeve.sma.SmaException;
import com.neeve.tools.interactive.commands.AnnotatedCommand;

abstract class Common extends AnnotatedCommand implements IEventHandler {
    @Option(shortForm = 'u', longForm = "username", required = true, description = "The bus binding username")
    String username;

    @Option(shortForm = 'b', longForm = "busDescriptor", required = true, description = "the messaging bus connection descriptor")
    String busDescriptorString;

    @Option(shortForm = 'c', longForm = "count", defaultValue = "-1", description = "number of messages to send if less than 1 then unlimited")
    protected int count;

    @Option(shortForm = 'k', longForm = "channelKey", description = "the channel key")
    String channelKey;

    @Option(shortForm = 'f', longForm = "channelFilter", description = "the channel filter")
    String channelFilter;

    @Option(shortForm = 'q', longForm = "qos", defaultValue = "Guaranteed", description = "the delivery QOS")
    MessageChannel.Qos qos;

    @Option(shortForm = 'e', longForm = "encoding", defaultValue = "xbuf2", description = "the encoding type")
    String encoding;

    protected MessageBusBinding binding;
    protected MessageChannel channel;

    static {
        System.setProperty("msg.latency.stats", "true");
        System.setProperty("nv.link.network.stampiots", "true");
        System.setProperty("nv.discovery.descriptor", "local://discovery&initWaitTime=0&maxEntityAge=5000");
        System.setProperty("nv.time.usenative", "true");
        try {
            final MessageBusDescriptor busDescriptor = MessageBusDescriptor.create("nvx-perf-sma");
            final MessageChannelDescriptor channelDescriptor = MessageChannelDescriptor.create("default", busDescriptor);
            busDescriptor.addChannel(channelDescriptor);
            busDescriptor.save(ConfigRepositoryFactory.getInstance().getDefaultRepository(), null);
            MessageViewFactoryRegistry.getInstance().registerMessageViewFactory(new com.neeve.perf.serialization.rumi.xbuf2.CarFactory());
        }
        catch (SmaException e) {
            throw new RuntimeException(e);
        }
    }

    public final void execute() throws Exception {
        doRun();
    }

    protected abstract void doRun() throws Exception;

    /**
     * Connects to the bus. 
     *  
     * @param join True if the channel should be joined. 
     *  
     * @throws Exception If there is an error connecting to the bus. 
     */
    protected void connect(final boolean join) throws Exception {
        final MessageBusDescriptor busDescriptor = MessageBusDescriptor.load(ConfigRepositoryFactory.getInstance().getDefaultRepository(), "nvx-perf-messaging", null);
        final MessageChannelDescriptor channelDescriptor = busDescriptor.getChannel("default");
        busDescriptor.setProviderConfig(busDescriptorString);
        channelDescriptor.setChannelId((short)1);
        channelDescriptor.setChannelQos(qos);
        channelDescriptor.setChannelKey(channelKey);
        channelDescriptor.setChannelFilter(channelFilter);
        binding = MessageBusBindingFactory.getInstance().createBinding(username, busDescriptor, this);
        channel = binding.getMessageChannel("default");
        if (join) {
            channel.join(0);
        }
        binding.start();
    }
}
