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
    protected String _username;

    @Option(shortForm = 'd', longForm = "descriptor", required = true, description = "the messaging bus connection descriptor")
    protected String _descriptor;

    @Option(shortForm = 'c', longForm = "testCount", defaultValue = "3000000", description = "number of messages to send if less than 1 then unlimited")
    protected int _testCount;

    @Option(shortForm = 'r', longForm = "testRate", required = true, defaultValue = "100000", description = "The send rate. If less than 1 then unlimited")
    protected int _testRate;

    @Option(shortForm = 'k', longForm = "channelKey", description = "the channel key")
    protected String _channelKey;

    @Option(shortForm = 'f', longForm = "channelFilter", description = "the channel filter")
    protected String _channelFilter;

    @Option(shortForm = 'q', longForm = "channelQos", defaultValue = "Guaranteed", description = "the delivery QOS")
    protected MessageChannel.Qos _channelQos;

    @Option(shortForm = 'e', longForm = "encoding", defaultValue = "rumi.xbuf2", description = "the encoding type")
    protected String _encoding;

    @Option(shortForm = 'i', longForm = "printIntervalStats", description = "whether to output stats at periodic intervals instead of only at the end")
    protected boolean _printIntervalStats;

    @Option(shortForm = 'f', longForm = "dontWriteLatenciesToFile", description = "whether to suppress writing latency values to a file")
    protected boolean _dontWriteLatenciesToFile;

    protected MessageBusBinding _binding;
    protected MessageChannel _channel;

    static {
        try {
            final MessageBusDescriptor busDescriptor = MessageBusDescriptor.create("nvx-perf-messaging");
            final MessageChannelDescriptor channelDescriptor = MessageChannelDescriptor.create("default", busDescriptor);
            busDescriptor.addChannel(channelDescriptor);
            busDescriptor.save(ConfigRepositoryFactory.getInstance().getDefaultRepository(), null);
            MessageViewFactoryRegistry.getInstance().registerMessageViewFactory(new com.neeve.perf.serialization.rumi.xbuf2.CarFactory());
        }
        catch (SmaException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Connect to the messaging bus. 
     *  
     * @param join True if the channel should be joined. 
     *  
     * @throws Exception If there is an error connecting to the bus. 
     */
    protected void connect(final boolean join) throws Exception {
        final MessageBusDescriptor busDescriptor = MessageBusDescriptor.load(ConfigRepositoryFactory.getInstance().getDefaultRepository(), "nvx-perf-messaging", null);
        final MessageChannelDescriptor channelDescriptor = busDescriptor.getChannel("default");
        busDescriptor.setProviderConfig(_descriptor);
        channelDescriptor.setChannelId((short)1);
        channelDescriptor.setChannelQos(_channelQos);
        channelDescriptor.setChannelKey(_channelKey);
        channelDescriptor.setChannelFilter(_channelFilter);
        _binding = MessageBusBindingFactory.getInstance().createBinding(_username, busDescriptor, this);
        _channel = _binding.getMessageChannel("default");
        if (join) {
            _channel.join(0);
        }
        _binding.start();
    }
}
