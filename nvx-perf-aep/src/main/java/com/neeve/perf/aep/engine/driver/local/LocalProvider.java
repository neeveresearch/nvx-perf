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

import java.util.Properties;

import com.neeve.event.*;
import com.neeve.sma.*;
import com.neeve.sma.impl.*;

final public class LocalProvider extends MessagingProviderBase {
    private LocalProvider(final String name, final Properties props) {
        super(null, name, props);
    }

    final public MessageBusBinding doCreateBinding(final String userName,
                                                   final MessageBusDescriptor descriptor,
                                                   final IEventHandler eventHandler) throws SmaException {
        try {
            return new LocalMessageBusBinding(userName, descriptor, eventHandler);
        }
        catch (Throwable e) {
            throw new SmaException(e);
        }
    }

    final public static MessagingProvider create(final String name, final Properties props) throws SmaException {
        return new LocalProvider(name, props);
    }
}
