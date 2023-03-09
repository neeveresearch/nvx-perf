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
package com.neeve.perf.aep.messaging;

import java.util.Map;
import java.util.Properties;

import com.neeve.aep.AepEngineDescriptor;
import com.neeve.aep.AepBusConnection;
import com.neeve.config.Config;
import com.neeve.sma.MessageBusDescriptor;
import com.neeve.sma.MessageChannel;
import com.neeve.sma.MessageChannelDescriptor;
import com.neeve.sma.MessageViewFactoryRegistry;

final class Configurer {
    final private static String MESSAGES_CHANNEL_NAME = "messages";

    final private static MessageBusDescriptor configureMessageBus(final String busName) throws Exception {
        // create and save the messaging descriptor to the config repo
        // ...create bus
        MessageBusDescriptor busDescriptor = MessageBusDescriptor.create("perf-aep");
        // ...add 'messages' channel to bus
        MessageChannelDescriptor channelDescriptor = MessageChannelDescriptor.create(MESSAGES_CHANNEL_NAME, busDescriptor);
        channelDescriptor.setChannelId((short)1);
        channelDescriptor.setChannelQos(MessageChannel.Qos.valueOf(Config.getValue("perf.aep.channel." + MESSAGES_CHANNEL_NAME + ".qos", MessageChannel.Qos.Guaranteed.toString())));
        channelDescriptor.setChannelKey(Config.getValue("perf.aep.channel." + MESSAGES_CHANNEL_NAME + ".key", null));
        busDescriptor.addChannel(channelDescriptor);

        // done
        return busDescriptor;
    }

    final static MessageBusDescriptor configure(final String appName, 
                                                final boolean receive,
                                                final Map<String, AepEngineDescriptor.ChannelConfig> channelsConfig, 
                                                final Properties props) throws Exception {
        // initialize config environment
        Config.initializeEnvironment();

        // configure messaging bus used for commmunication between services
        final MessageBusDescriptor busDescriptor = configureMessageBus(appName);

        // register message view factory
        MessageViewFactoryRegistry.getInstance().registerMessageViewFactory("com.neeve.perf.aep.messages.MessageFactory");

        // prepare channel config
        // ...... messages channel
        AepEngineDescriptor.ChannelConfig channelConfig = new AepEngineDescriptor.ChannelConfig();
        channelConfig.setJoin(receive);
        channelConfig.setFilter(Config.getValue("perf.aep.channel." + MESSAGES_CHANNEL_NAME + "." + appName + ".filter", null));
        channelsConfig.put(MESSAGES_CHANNEL_NAME, channelConfig);

        // prepare connection properties
        props.setProperty(AepBusConnection.PROPNAME_PROVIDER_DESCRIPTOR, Config.getValue("perf.aep.bus." + appName + ".descriptor", Config.getValue("perf.aep.bus.descriptor", null)));
        props.setProperty(AepBusConnection.PROPNAME_DETACHED, "false");

        // done
        return busDescriptor;
    }
}
