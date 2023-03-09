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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.neeve.aep.AepEngineDescriptor;
import com.neeve.aep.AepBusConnection;
import com.neeve.aep.event.AepBusBindingUpEvent;
import com.neeve.config.Config;
import com.neeve.event.Event;
import com.neeve.event.IEventHandler;
import com.neeve.sma.MessageBusBinding;
import com.neeve.sma.MessageBusDescriptor;
import com.neeve.sma.MessageLatencyManager;
import com.neeve.sma.MessageView;
import com.neeve.sma.MessageViewTags;
import com.neeve.sma.event.MessageEvent;
import com.neeve.util.UtlTime;

public class Receiver implements IEventHandler {
    final private class LatencyRecorder implements MessageLatencyManager.UpdateListener {
        final private int _expectedMsgCount;
        final private int[] _w2rLatencies;
        private int _latencyCount;

        LatencyRecorder(final int expectedMsgCount) {
            _expectedMsgCount = expectedMsgCount;
            _w2rLatencies = new int[_expectedMsgCount];
        }

        final private void writeDataToFile(final String filename, final int[] data) {
            try {
                FileOutputStream fos = new FileOutputStream(new File(filename));
                DataOutputStream dos = new DataOutputStream(fos);
                for (int i = 0; i < data.length; i++) {
                    dos.writeInt(i);
                    dos.writeInt(data[i]);
                }
                dos.flush();
                dos.close();
                System.out.println("wrote: " + filename + ", n-data points: " + data.length);
            }
            catch (FileNotFoundException ex_) {
                ex_.printStackTrace();
            }
            catch (IOException ex_) {
                ex_.printStackTrace();
            }
        }

        final private void writeLatencies() {
            System.out.println("Writing latencies... date/time:" + LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
            writeDataToFile("w2r.bin", _w2rLatencies);
            System.out.println("Finished writing latencies");
        }

        @Override
        final public void onUpdate(final MessageBusBinding binding, final MessageView view, final MessageLatencyManager.MessagingDirection direction) {
            if (direction == MessageLatencyManager.MessagingDirection.Inbound) {
                _w2rLatencies[_latencyCount] = (int)(view.getReceiveTs() - view.getPostWireTs());
                _latencyCount += 1;
                if (_latencyCount == _expectedMsgCount) {
                    writeLatencies();
                }
            }
        }
    }

    final private static String APP_NAME = "receiver";
    final private MessageBusDescriptor _busDescriptor;
    final private Map<String, AepEngineDescriptor.ChannelConfig> _channelsConfig;
    final private Properties _props;
    final private LatencyRecorder _latencyRecorder;

    private Receiver(final MessageBusDescriptor busDescriptor,
                     final Map<String, AepEngineDescriptor.ChannelConfig> channelsConfig,
                     final Properties props) {
        _busDescriptor = busDescriptor;
        _channelsConfig = channelsConfig;
        _props = props;
        _latencyRecorder = new LatencyRecorder(Config.getValue("perf.aep.sendCount", 10000));
    }

    final private void run() throws Exception {
        // open the bus connection
        final AepBusConnection connection = AepBusConnection.create(APP_NAME, _busDescriptor, _channelsConfig, this, _props);
        connection.open();

        // block and wait. the remainder of the app is driven by inbound messages
        Thread.sleep(Long.MAX_VALUE);
    }

    @Override
    public void onEvent(final Event event) {
        if (event instanceof MessageEvent) {
            final MessageView message = ((MessageEvent)event).getMessageView();
            message.setReceiveTs(UtlTime.now());
            final MessageLatencyManager latencyManager = (MessageLatencyManager)message.getTag(MessageViewTags.TAG_SMA_LATENCY_MANAGER);
            if (latencyManager != null) latencyManager.update(message, MessageLatencyManager.MessagingDirection.Inbound);
        }
        else {
            if (event instanceof AepBusBindingUpEvent) {
                ((AepBusBindingUpEvent)event).getMessageBusBinding().getLatencyManager().setUpdateListener(_latencyRecorder);
            }
            System.out.println("Event: " + event);
        }
    }

    public static void main(final String args[]) {
        try {
            // configure
            final Map<String, AepEngineDescriptor.ChannelConfig> channelsConfig = new HashMap<String, AepEngineDescriptor.ChannelConfig>();
            final Properties props = new Properties();
            MessageBusDescriptor busDescriptor = Configurer.configure(APP_NAME, true, channelsConfig, props);

            // run
            new Receiver(busDescriptor, channelsConfig, props).run();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
