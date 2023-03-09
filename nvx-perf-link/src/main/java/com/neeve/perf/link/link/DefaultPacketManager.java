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

import com.neeve.util.*;
import com.neeve.stats.*;
import com.neeve.pkt.*;
import com.neeve.pkt.types.*;
import com.neeve.link.*;

/**
 * Implements the default packet manager - creates a single data packet that
 * is repeatedly sent and does ot post receive processing.
 */
final public class DefaultPacketManager implements IPacketManager {
    /*
     * Private scope members
     */
    final private StringBuilder sb = new StringBuilder();
    private PktPacket packet;
    private int dataSize = 128;
    private boolean same = false; // use same packet over and over
    private long last;

    /*
     * Create a new packet
     */
    final PktPacket createPacket() {
        final PktPacket packet = PktFactory.getInstance().createPacket(PktBodyTypesBase.DATA);
        final PktBodyData body = (PktBodyData)packet.getBody();
        body.setBufferLength(dataSize);
        body.getBuffer().putLong(0, UtlTime.now());
        return packet;
    }

    /**
     * Implementation of {@link IPacketManager#setProperty}
     */
    final public void setProperty(final String name, final String value) {
        if (name.equals("size")) {
            dataSize = Math.max(Integer.parseInt(value), 8); // to accomodate the timestamp
        }
        else if (name.equals("same")) {
            same = Boolean.parseBoolean(value);
        }
    }

    /**
     * Implementation of {@link IPacketManager#getPacketForSend}
     */
    final public PktPacket getPacketForSend() {
        if (same) {
            if (packet == null) {
                packet = createPacket();
                packet.acquire(); // to prevent it being released to the pool
            }
            return packet;
        }
        else {
            return createPacket();
        }
    }

    /**
     * Implementation of {@link IPacketManager#onReceive}
     */
    final public boolean onReceive(final PktPacket packet) {
        return false;
    }
}
