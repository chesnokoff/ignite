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

package org.apache.ignite.internal.marshaller.optimized;

import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class OptimizedMarshallerEnumSelfTest extends GridCommonAbstractTest {

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEnumSerialisation() throws Exception {
        Marshaller marsh = initTestMarshallerContext(new OptimizedMarshaller());

        byte[] bytes = marsh.marshal(TestEnum.Bond);

        TestEnum unmarshalled = marsh.unmarshal(bytes, Thread.currentThread().getContextClassLoader());

        assertEquals(TestEnum.Bond, unmarshalled);
        assertEquals(TestEnum.Bond.desc, unmarshalled.desc);
    }

    /** */
    private enum TestEnum {
        /** */
        Equity("Equity") {
            /** {@inheritDoc} */
            @Override public String getTestString() {
                return "eee";
            }
        },

        /** */
        Bond("Bond") {
            /** {@inheritDoc} */
            @Override public String getTestString() {
                return "qqq";
            }
        };

        /** */
        public final String desc;

        /** */
        TestEnum(String desc) {
            this.desc = desc;
        }

        /** */
        public abstract String getTestString();
    }
}
