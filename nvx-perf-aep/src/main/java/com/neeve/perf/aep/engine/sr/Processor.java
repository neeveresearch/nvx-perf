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
package com.neeve.perf.aep.engine.sr;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import com.neeve.aep.AepEngine;
import com.neeve.aep.AepEngineDescriptor;
import com.neeve.aep.AepMessageSender;
import com.neeve.aep.IAepApplicationStateFactory;
import com.neeve.aep.annotations.EventHandler;
import com.neeve.config.Config;
import com.neeve.config.VMConfigurer;
import com.neeve.perf.aep.engine.LatencyRecorder;
import com.neeve.perf.aep.engine.state.Repository;
import com.neeve.perf.aep.messages.Event;
import com.neeve.perf.aep.messages.Message;
import com.neeve.sma.MessageView;
import com.neeve.util.UtlTailoring;
import com.neeve.util.UtlTime;

final public class Processor {
    final private AepMessageSender _messageSender;
    final private AepEngine _engine;

    public Processor(final AepEngineDescriptor engineDescriptor) {
        // create message sender
        _messageSender = AepMessageSender.create();

        // create engine
        _engine = AepEngine.create(engineDescriptor, 
                                   getStateFactory(),
                                   new HashSet<Object>(Arrays.asList(new Object[] { this })), 
                                   null, 
                                   new HashSet<AepMessageSender>(Arrays.asList(new AepMessageSender[] { _messageSender })), 
                                   null);
    }

    final private IAepApplicationStateFactory getStateFactory() {
        return new IAepApplicationStateFactory() {
            @Override
            final public Repository createState(MessageView view) {
                return Repository.create();
            }
        };
    }

    final private void run() throws Exception {
        // start engine
        _engine.start();

        // block and wait. the remainder of the app is driven by inbound messages
        Thread.sleep(Long.MAX_VALUE);
    }

    @EventHandler
    final public void onMessage(final Message message, final Repository repository) throws Exception {
        // record w2b
        final long ts1 = UtlTime.now();
        LatencyRecorder.recordW2b(ts1 - message.getPostWireTs());

        // prepare outbound event
        final Event event = Event.create();
        final Event.Serializer serializer = event.serializer(1024);
        serializer.key("key").count(1).done();
        final long ts2 = UtlTime.now();
        LatencyRecorder.recordB(ts2 - ts1);

        // send outbound
        final long ts3 = UtlTime.now();
        event.setPostWireTs(message.getPostWireTs());
        _messageSender.sendMessage(1, event);
        LatencyRecorder.recordS(UtlTime.now() - ts3);
    }

    final public static void main(final String args[]) {
        try {
            // load config
            VMConfigurer.configure(new File(Paths.get(Config.getRootDirectory().toString(), "conf", "config.xml").toString()), UtlTailoring.ENV_SUBSTITUTION_RESOLVER);
            Config.initializeEnvironment();
            final AepEngineDescriptor engineDescriptor = AepEngineDescriptor.load("processor");

            // run
            new Processor(engineDescriptor).run();
        }
        catch (Throwable e) {
            e.printStackTrace();
        }
    }
}

