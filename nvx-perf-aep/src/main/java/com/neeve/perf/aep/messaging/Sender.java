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
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

import com.neeve.aep.AepEngineDescriptor;
import com.neeve.aep.AepBusConnection;
import com.neeve.aep.event.AepBusBindingUpEvent;
import com.neeve.config.Config;
import com.neeve.event.Event;
import com.neeve.event.IEventHandler;
import com.neeve.perf.aep.messages.Message;
import com.neeve.sma.MessageBusBinding;
import com.neeve.sma.MessageBusDescriptor;
import com.neeve.sma.MessageLatencyManager;
import com.neeve.sma.MessageView;
import com.neeve.sma.MessageViewTags;
import com.neeve.trace.Tracer;
import com.neeve.util.UtlGovernor;
import com.neeve.util.UtlThrowable;
import com.neeve.util.UtlTime;

public class Sender implements IEventHandler {
    final private class LatencyRecorder implements MessageLatencyManager.UpdateListener {
        final private int _expectedMsgCount;
        final private int[] _c2wLatencies;
        final private int[] _wLatencies;
        final private int[] _sLatencies;
        private int _latencyCount;

        LatencyRecorder(final int expectedMsgCount) {
            _expectedMsgCount = expectedMsgCount;
            _c2wLatencies = new int[_expectedMsgCount];
            _wLatencies = new int[_expectedMsgCount];
            _sLatencies = new int[_expectedMsgCount];
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

        final void updateS(final int val) {
            _sLatencies[_latencyCount++] = val;
        }

        @Override
        final public void onUpdate(final MessageBusBinding binding, final MessageView view, final MessageLatencyManager.MessagingDirection direction) {
            if (direction == MessageLatencyManager.MessagingDirection.Outbound) {
                _c2wLatencies[_latencyCount] = (int)(view.getPreWireTs() - view.getCreateTs());
                _wLatencies[_latencyCount] = (int)(view.getPostWireSendTs() - view.getPreWireTs());
            }
        }

        public void writeLatencies() {
            System.out.println("Writing latencies... date/time:" + LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
            writeDataToFile("c2w.bin", _c2wLatencies);
            writeDataToFile("w.bin", _wLatencies);
            writeDataToFile("s.bin", _sLatencies);
            System.out.println("Finished writing latencies");
        }
    }

    final private static String APP_NAME = "sender";
    final private int _keyCount;
    final private int _sendCount;
    final private int _sendRate;
    final private Tracer _tracer;
    final private String[] _keys;
    final private Random _random;
    final private MessageBusDescriptor _busDescriptor;
    final private Map<String, AepEngineDescriptor.ChannelConfig> _channelsConfig;
    final private Properties _props;
    final private LatencyRecorder _latencyRecorder;

    private Sender(final MessageBusDescriptor busDescriptor,
                   final Map<String, AepEngineDescriptor.ChannelConfig> channelsConfig,
                   final Properties props) {
        _keyCount = Config.getValue("perf.aep.keyCount", 1000);
        _sendCount = Config.getValue("perf.aep.sendCount", 10000);
        _sendRate = Config.getValue("perf.aep.sendRate", 1000);
        _tracer = Tracer.get("sender");
        _random = new Random(System.currentTimeMillis());
        _keys = new String[_keyCount];
        for (int i = 0; i < _keys.length ; i++) {
            _keys[i] = RandomStringUtils.random(10, true, true);
        }
        _busDescriptor = busDescriptor;
        _channelsConfig = channelsConfig;
        _props = props;
        _latencyRecorder = new LatencyRecorder(_sendCount);
    }

    final private Message createMessage() {
        final Message message = Message.create();
        final Message.Serializer serializer = message.serializer(1024);
        serializer.key(_keys[_random.nextInt(_keyCount)]).done();
        return message;
    }

    final private void sendMessages(final AepBusConnection connection) throws Exception {
        _tracer.log("Sending " + _sendCount + " messages at " + _sendRate + "/sec", Tracer.Level.INFO);
        final UtlGovernor sendGoverner = new UtlGovernor(_sendRate);
        int sent = 0;
        while (sent++ < _sendCount) {
            final Message message = createMessage();
            try {
                long pre = System.nanoTime();
                connection.send(connection.getChannel("messages"), message, 0);
                _latencyRecorder.updateS((int)(System.nanoTime() - pre));
                if (sent % 10000 == 0) {
                    _tracer.log("Sent " + sent + " messages", Tracer.Level.INFO);
                }
            }
            finally { 
                message.dispose();
            }
            sendGoverner.blockToNext();
        }
        _latencyRecorder.writeLatencies();
    }

    final private void run() throws Exception {
        // open the bus connection 
        final AepBusConnection connection = AepBusConnection.create(APP_NAME, _busDescriptor, _channelsConfig, this, _props);
        connection.open();

        // send messages
        sendMessages(connection);
    }

    @Override
    public void onEvent(final Event event) {
        if (event instanceof AepBusBindingUpEvent) {
            ((AepBusBindingUpEvent)event).getMessageBusBinding().getLatencyManager().setUpdateListener(_latencyRecorder);
        }
        System.out.println("Event: " + event);
    }

    final public static void main(final String args[]) {
        try {
            // configure
            final Map<String, AepEngineDescriptor.ChannelConfig> channelsConfig = new HashMap<String, AepEngineDescriptor.ChannelConfig>();
            final Properties props = new Properties();
            MessageBusDescriptor busDescriptor = Configurer.configure(APP_NAME, false, channelsConfig, props);

            // run
            new Sender(busDescriptor, channelsConfig, props).run();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
