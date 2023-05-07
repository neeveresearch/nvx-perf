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
package com.neeve.perf.ods;

import java.util.Properties;

import com.eaio.uuid.UUID;

import com.neeve.ods.IStoreBinding;
import com.neeve.ods.IStoreEvent;
import com.neeve.ods.IStoreEventHandler;
import com.neeve.ods.StoreBinding;
import com.neeve.ods.StoreDescriptor;
import com.neeve.ods.StoreObjectFactoryRegistry;
import com.neeve.ods.StorePersisterDescriptor;
import com.neeve.ods.StoreReplicatorDescriptor;

abstract class Common implements IStoreEventHandler {
    final protected IStoreBinding _store;

    /**
     * Constructor
     */
    protected Common(final boolean enablePersistence,
                     final Properties persisterProperties,
                     final boolean enableReplication, 
                     final Properties replicatorProperties,
                     final int flags) throws Exception {
        final String storeName = "perf";
        final String memberName = new UUID().toString();
        StoreDescriptor storeDescriptor = StoreDescriptor.create(storeName);
        storeDescriptor.setPersistenceQuorum(1);
        if (enableReplication) {
            StoreReplicatorDescriptor replicatorDescriptor = StoreReplicatorDescriptor.create(storeName);
            replicatorDescriptor.setProperties(persisterProperties);
            replicatorDescriptor.save();
            storeDescriptor.setReplicator(storeName);
        }
        if (enablePersistence) {
            StorePersisterDescriptor persisterDescriptor = StorePersisterDescriptor.create(storeName, com.neeve.rog.log.RogLog.class.getName());
            persisterDescriptor.setProperties(persisterProperties);
            persisterDescriptor.save();
            storeDescriptor.setPersister(storeName);
        }
        (_store = StoreBinding.create(memberName, storeDescriptor, this, flags)).open();
    }

    /**
     * Event handler (noop)
     */
    @Override
    final public void onEvent(final IStoreEvent event) {
    }
}
