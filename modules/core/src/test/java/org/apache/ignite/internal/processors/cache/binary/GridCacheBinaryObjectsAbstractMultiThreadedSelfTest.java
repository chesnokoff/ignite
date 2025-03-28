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

package org.apache.ignite.internal.processors.cache.binary;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Test for binary objects stored in cache.
 */
public abstract class GridCacheBinaryObjectsAbstractMultiThreadedSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int THREAD_CNT = 64;

    /** */
    private static final AtomicInteger idxGen = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cacheCfg.setCacheMode(cacheMode());
        cacheCfg.setAtomicityMode(atomicityMode());
        cacheCfg.setNearConfiguration(nearConfiguration());
        cacheCfg.setWriteSynchronizationMode(writeSynchronizationMode());

        cfg.setCacheConfiguration(cacheCfg);

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setTypeConfigurations(Arrays.asList(
            new BinaryTypeConfiguration(TestObject.class.getName())));

        cfg.setBinaryConfiguration(bCfg);

        return cfg;
    }

    /**
     * @return Sync mode.
     */
    protected CacheWriteSynchronizationMode writeSynchronizationMode() {
        return PRIMARY_SYNC;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(gridCount());
    }

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return Atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /**
     * @return Distribution mode.
     */
    protected abstract NearCacheConfiguration nearConfiguration();

    /**
     * @return Grid count.
     */
    protected int gridCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("BusyWait")
    @Test
    public void testGetPut() throws Exception {
        final AtomicBoolean flag = new AtomicBoolean();

        final LongAdder cnt = new LongAdder();

        IgniteInternalFuture<?> f = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    int threadId = idxGen.getAndIncrement() % 2;

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!flag.get()) {
                        IgniteCache<Object, Object> c = jcache(rnd.nextInt(gridCount()));

                        switch (threadId) {
                            case 0:
                                // Put/get/remove binary -> binary.

                                c.put(new TestObject(rnd.nextInt(10000)), new TestObject(rnd.nextInt(10000)));

                                IgniteCache<Object, Object> p2 = ((IgniteCacheProxy<Object, Object>)c).keepBinary();

                                BinaryObject v = (BinaryObject)p2.get(new TestObject(rnd.nextInt(10000)));

                                if (v != null)
                                    v.deserialize();

                                c.remove(new TestObject(rnd.nextInt(10000)));

                                break;

                            case 1:
                                // Put/get int -> binary.
                                c.put(rnd.nextInt(10000), new TestObject(rnd.nextInt(10000)));

                                IgniteCache<Integer, BinaryObject> p4 = ((IgniteCacheProxy<Object, Object>)c).keepBinary();

                                BinaryObject v1 = p4.get(rnd.nextInt(10000));

                                if (v1 != null)
                                    v1.deserialize();

                                p4.remove(rnd.nextInt(10000));

                                break;

                            default:
                                assert false;
                        }

                        cnt.add(3);
                    }

                    return null;
                }
            },
            THREAD_CNT
        );

        for (int i = 0; i < 30 && !f.isDone(); i++)
            Thread.sleep(1000);

        flag.set(true);

        f.get();

        info("Operations in 30 sec: " + cnt.sum());
    }

    /**
     */
    private static class TestObject implements Binarylizable, Serializable {
        /** */
        private int val;

        /**
         */
        private TestObject() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        private TestObject(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof TestObject && ((TestObject)obj).val == val;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("val", val);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            val = reader.readInt("val");
        }
    }
}
