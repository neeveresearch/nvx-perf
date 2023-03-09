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
package com.neeve.perf.aep.engine.es;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import jargs.gnu.CmdLineParser;

import com.neeve.aep.AepEngine;
import com.neeve.aep.AepEngineDescriptor;
import com.neeve.aep.AepMessageSender;
import com.neeve.aep.annotations.EventHandler;
import com.neeve.config.Config;
import com.neeve.config.VMConfigurer;
import com.neeve.perf.aep.messages.Event;
import com.neeve.perf.aep.messages.Message;
import com.neeve.server.embedded.EmbeddedXVM;
import com.neeve.server.config.SrvConfigDescriptor;
import com.neeve.server.app.annotations.AppHAPolicy;
import com.neeve.server.app.annotations.AppInjectionPoint;
import com.neeve.util.UtlTailoring;
import com.neeve.util.UtlTime;

@AppHAPolicy(value = AepEngine.HAPolicy.EventSourcing)
final public class Processor {
    private AepEngine _engine;
    private AepMessageSender _messageSender;
    final private boolean _recordLatencies; 

    private Processor(final boolean recordLatencies) {
        _recordLatencies = recordLatencies;
    }

    public Processor() {
        this(false);
    }

    @EventHandler
    final public void onMessage(final Message message) throws Exception {
        // prepare outbound event
        final Event event = Event.create();
        final Event.Serializer serializer = event.serializer(1024);
        serializer.key("key").count(1).done();

        // send outbound
        event.setPostWireTs(message.getPostWireTs());
        _messageSender.sendMessage(1, event);
    }

	@AppInjectionPoint
	final public void setEngine(AepEngine engine) {
		_engine = engine;
	}

	@AppInjectionPoint
	final public void setMessageSender(AepMessageSender messageSender) {
		_messageSender = messageSender;
	}

    final private static void printUsage() {
        System.err.println("Usage Processor");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-s, --server} Launch in a Rumi server]");
        System.err.println("   If specified, then the processor launches an embedded Rumi server to launch and manage the service. (default=false)");
        System.err.println("--------------------------------------------------------------------------------------------------------------------");
        System.err.println(" [{-h, --help} print this help string]");
    }

    final public static void main(final String args[]) {
        final CmdLineParser parser = new CmdLineParser();
        final CmdLineParser.Option serverOption = parser.addBooleanOption('s', "server");
        try {
            // parse
            parser.parse(args);
            final boolean launchInServer = (Boolean)parser.getOptionValue(serverOption, false);
            
            // load config
            VMConfigurer.configure(new File(Paths.get(Config.getRootDirectory().toString(), "conf", "config.xml").toString()), UtlTailoring.ENV_SUBSTITUTION_RESOLVER);

            // how we start the app depends on whether telemtry is enabled or not
            if (launchInServer) {
                // start the server
                SrvConfigDescriptor serverDescriptor = SrvConfigDescriptor.load("processor-1a");
                EmbeddedXVM server = EmbeddedXVM.create(serverDescriptor);
                server.start();
            }
            else {
                // initialize environment
                Config.initializeEnvironment();

                // create processor
                final Processor processor = new Processor(true);

                // create the engine
                final AepMessageSender sender = AepMessageSender.create();
                final AepEngineDescriptor engineDescriptor = AepEngineDescriptor.load("processor");
                engineDescriptor.setHAPolicy(AepEngine.HAPolicy.EventSourcing);
                final AepEngine engine = AepEngine.create(engineDescriptor, 
                                                          null,
                                                          new HashSet<Object>(Arrays.asList(new Object[] {processor})), 
                                                          null,
                                                          new HashSet<AepMessageSender>(Arrays.asList(new AepMessageSender[] { sender })), 
                                                          null);


                // prepare the processor
                processor.setMessageSender(sender);
                processor.setEngine(engine);

                // start engine
                engine.start();
            }

            // block and wait. the remainder of the app is driven by inbound messages
            // driven by the processor's engine
            Thread.sleep(Long.MAX_VALUE);
        }
        catch (Throwable e) {
            e.printStackTrace();
        }
    }
}

