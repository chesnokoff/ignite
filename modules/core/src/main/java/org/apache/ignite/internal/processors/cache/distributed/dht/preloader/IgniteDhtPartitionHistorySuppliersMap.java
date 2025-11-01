/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteDhtPartitionHistorySuppliersMap implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 510;

    /** */
    private static final IgniteDhtPartitionHistorySuppliersMap EMPTY = new IgniteDhtPartitionHistorySuppliersMap();

    /** */
    @Order(value = 0, method = "historySuppliers")
    private Map<UUID, PartitionReservationsMap> map;

    /**
     * @return Empty map.
     */
    public static IgniteDhtPartitionHistorySuppliersMap empty() {
        return EMPTY;
    }

    /**
     * @param grpId Group ID.
     * @param partId Partition ID.
     * @param cntrSince Partition update counter since history supplying is requested.
     * @return List of supplier UUIDs or empty list if haven't these.
     */
    public synchronized List<UUID> getSupplier(int grpId, int partId, long cntrSince) {
        if (map == null)
            return Collections.EMPTY_LIST;

        List<UUID> suppliers = new ArrayList<>();

        for (Map.Entry<UUID, PartitionReservationsMap> e : map.entrySet()) {
            UUID supplierNode = e.getKey();

            Long historyCounter = e.getValue().get(new GroupPartitionIdPair(grpId, partId));

            if (historyCounter != null && historyCounter <= cntrSince)
                suppliers.add(supplierNode);
        }

        return suppliers;
    }

    /**
     * @param nodeId Node ID to check.
     * @return Reservations for the given node.
     */
    @Nullable public synchronized PartitionReservationsMap getReservations(UUID nodeId) {
        if (map == null)
            return null;

        return map.get(nodeId);
    }

    /**
     * @param nodeId Node ID.
     * @param grpId Cache group ID.
     * @param partId Partition ID.
     * @param cntr Partition counter.
     */
    public synchronized void put(UUID nodeId, int grpId, int partId, long cntr) {
        if (map == null)
            map = new HashMap<>();

        PartitionReservationsMap nodeMap = map.computeIfAbsent(nodeId, k -> new PartitionReservationsMap());

        nodeMap.put(new GroupPartitionIdPair(grpId, partId), cntr);
    }

    /**
     * @return {@code True} if empty.
     */
    public synchronized boolean isEmpty() {
        return map == null || map.isEmpty();
    }

    /**
     * @param that Other map to put.
     */
    public synchronized void putAll(IgniteDhtPartitionHistorySuppliersMap that) {
        map = that.map;
    }

    /**
     * @return Partition history suppliers map.
     */
    public Map<UUID, PartitionReservationsMap> historySuppliers() {
        return map;
    }

    /**
     * @param map Partition history suppliers map.
     */
    public void historySuppliers(Map<UUID, PartitionReservationsMap> map) {
        this.map = map;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteDhtPartitionHistorySuppliersMap.class, this);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
