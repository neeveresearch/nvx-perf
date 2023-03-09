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
package com.neeve.perf.aep.engine.driver.local;

import com.neeve.sma.MessageBusBinding;
import com.neeve.sma.MessageChannelDescriptor;
import com.neeve.sma.MessageView;
import com.neeve.sma.SmaException;
import com.neeve.sma.impl.MessageChannelBase;

final public class LocalMessageChannel extends MessageChannelBase {
    final LocalMessageBusBinding _binding;

    LocalMessageChannel(final MessageChannelDescriptor descriptor, final LocalMessageBusBinding binding) throws SmaException {
        super(null, descriptor, binding);
        _binding = binding;
    }

    @Override
    final protected boolean doSend(final MessageView view, final int flags) throws SmaException {
        _binding.send(view);
        return false;
    }

    @Override
    final protected void doJoin(final String[] filters, final int flags) throws SmaException {}

    @Override
    final protected void doLeave(final int flags) throws SmaException {}

    @Override
    final protected void doClose() throws SmaException {}

    @Override
    final public String getType() {
        return "Local";
    }
}
