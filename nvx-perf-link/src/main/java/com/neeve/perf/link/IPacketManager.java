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
package com.neeve.perf.link;

import com.neeve.pkt.PktPacket;

/**
 * Defines the packet generator. A packet generator is used by the link 
 * performance tools to do pre-send and post-receive operations.
 */
public interface IPacketManager {
    /**
     * Set an operating  property
     */
    public void setProperty(final String name, final String value);

    /**
     * Get a packet to send.
     */
    public PktPacket getPacketForSend() throws Exception;

    /**
     * Process a received packet
     * 
     * @return Return false in case not processed. If not processed by the 
     * manager, the tool will acquire and release a reference to the packet
     * to force it to be pooled if attached to a pool.
     */
    public boolean onReceive(final PktPacket packet);
}
