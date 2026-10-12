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
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks that a node with a different {@code partitionLossPolicy} of a statically configured cache is not allowed
 * to join the cluster.
 */
public class CachePartitionLossPolicyMismatchTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCacheConfiguration(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setPartitionLossPolicy(PartitionLossPolicy.IGNORE));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLossPolicyMatch() throws Exception {
        startGrid(0);
        startGrid(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionLossPolicyMismatch() throws Exception {
        startGrid(0);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(1));

        cfg.getCacheConfiguration()[0].setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE);

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(cfg), IgniteCheckedException.class,
            "Partition loss policy mismatch");
    }
}
