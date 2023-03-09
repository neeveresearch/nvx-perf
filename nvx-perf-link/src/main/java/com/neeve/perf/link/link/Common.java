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
package com.neeve.perf.link.link;

import java.lang.reflect.*;
import java.util.*;

import com.neeve.tools.interactive.commands.AnnotatedCommand;
import com.neeve.pkt.*;

/**
 * Base class for all senders and receivers.
 */
abstract class Common extends AnnotatedCommand {
    protected IPacketManager packetManager;

    @Option(shortForm = 's', longForm = "smasubhdr", defaultValue = "false", description = "include SMA subheader in packets?")
    protected boolean smasubhdr;

    @Option(shortForm = 'o', longForm = "odssubhdr", defaultValue = "false", description = "include ODS subheader in packets?")
    protected boolean odssubhdr;

    @Option(shortForm = 'r', longForm = "rate", required = false, defaultValue = "-1", description = "The send rate. If less than 1 then unlimited")
    int rate;

    @Option(shortForm = 'c', longForm = "count", required = false, defaultValue = "0", description = "The send count. If less than 1 then unlimited")
    long count;

    @Option(shortForm = 'p', longForm = "packetmanager", required = false, description = "The packet manager to load, default if omitted")
    String packetManagerClassName;

    @Option(shortForm = 'x', longForm = "packetmanagerprops", required = false, description = "A comma delimited lists of packet manager properties, e.g key1=val1,key2=val2")
    String packetManagerOptions;

    public final void execute() throws Exception {
        if (packetManagerClassName == null) {
            System.out.println("Using default (data) packet manager");
            this.packetManager = new DefaultPacketManager();
        }
        else {
            Class<?> packetManagerClass = Class.forName(packetManagerClassName);
            System.out.println("Using the '" + packetManagerClassName + "' packet manager...");
            Constructor<?> constructor = packetManagerClass.getConstructor(new Class[0]);
            this.packetManager = (IPacketManager)constructor.newInstance(new Object[0]);
        }

        if (packetManagerOptions != null) {
            StringTokenizer tok = new StringTokenizer(packetManagerOptions, ",");
            while (tok.hasMoreTokens()) {
                String token = tok.nextToken();
                String[] propPair = token.split("=");
                if (propPair.length != 2) {
                    throw new IllegalArgumentException("Invalid packet manager property: " + token);
                }
                System.out.println("Setting property '" + propPair[0] + "=" + propPair[1] + "'");
                this.packetManager.setProperty(propPair[0], propPair[1]);
            }
        }

        doRun();
    }

    protected abstract void doRun() throws Exception;

    final protected PktPacket prepHeaders(final PktPacket packet) {
        if (smasubhdr) {
            PktSubheaderSMA.prepare(packet, (byte)2, (short)0, (short)0, 0, 0, (short)0, null);
        }
        if (odssubhdr) {
            PktSubheaderODS.prepare(packet, null, (short)0, (short)0, (short)0, 0l, 0l, 0l, 0l, (byte)0);
        }
        return packet;
    }
}
