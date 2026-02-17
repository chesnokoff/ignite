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

package org.apache.ignite.internal.processors.performancestatistics;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_LOAD;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_LOAD_ALL;
import static org.apache.ignite.internal.processors.performancestatistics.OperationType.CACHE_LOAD_CACHE;

/**
 * Tests performance statistics for cache store load operations.
 */
public class PerformanceStatisticsCacheStoreLoadTest extends AbstractPerformanceStatisticsTest {
    /** Ignite. */
    private static IgniteEx ignite;

    /** Test cache. */
    private static IgniteCache<Object, Object> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Object, Object> ccfg = defaultCacheConfiguration();

        ccfg.setReadThrough(true);
        ccfg.setCacheStoreFactory(FactoryBuilder.factoryOf(TestCacheStore.class));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrid(0);

        cache = ignite.cache(DEFAULT_CACHE_NAME);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheStoreLoadOperation() throws Exception {
        checkCacheStoreOperation(CACHE_LOAD, () -> cache.get(1001));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheStoreLoadAllOperation() throws Exception {
        checkCacheStoreOperation(CACHE_LOAD_ALL, () -> cache.getAll(new HashSet<>(Arrays.asList(1002, 1003))));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCacheStoreLoadCacheOperation() throws Exception {
        checkCacheStoreOperation(CACHE_LOAD_CACHE, () -> cache.loadCache(null));
    }

    /** Checks store operation in performance statistics. */
    private void checkCacheStoreOperation(OperationType op, Runnable action) throws Exception {
        long startTime = U.currentTimeMillis();

        cleanPerformanceStatisticsDir();

        startCollectStatistics();

        action.run();

        AtomicInteger ops = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void cacheOperation(UUID nodeId, OperationType type, int cacheId, long opStartTime,
                long duration) {
                if (type != op)
                    return;

                ops.incrementAndGet();

                assertEquals(ignite.context().localNodeId(), nodeId);
                assertEquals(CU.cacheId(DEFAULT_CACHE_NAME), cacheId);
                assertTrue(opStartTime >= startTime);
                assertTrue(duration >= 0);
            }
        });

        assertEquals("Unexpected operation count for " + op, 1, ops.get());
    }

    /** Test cache store. */
    public static class TestCacheStore implements CacheStore<Object, Object> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, @Nullable Object... args)
            throws CacheLoaderException {
            clo.apply(2000, 2000);
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            return key;
        }

        /** {@inheritDoc} */
        @Override public Map<Object, Object> loadAll(Iterable<? extends Object> keys) throws CacheLoaderException {
            Map<Object, Object> vals = new HashMap<>();

            for (Object key : keys)
                vals.put(key, key);

            return vals;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Object, ? extends Object> entry) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends Object, ? extends Object>> entries)
            throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void sessionEnd(boolean commit) {
            // No-op.
        }
    }
}
