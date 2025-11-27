/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rollingupgrade;

import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for rolling upgrade compatibility matrix.
 */
public class RollingUpgradeCompatibilityMatrixSelfTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testUpgradeAllowedByMatrix() throws Exception {
        RollingUpgradeCompatibilityMatrix matrix = RollingUpgradeCompatibilityMatrix.from(Collections.singleton(
            new RollingUpgradeCompatibilityRule("2.18.0", "2.18.1")
        ));

        RollingUpgradeCompatibilityChecker.validate(
            IgniteProductVersion.fromString("2.18.0"),
            IgniteProductVersion.fromString("2.18.1"),
            false,
            matrix,
            NullLogger.INSTANCE
        );
    }

    /** */
    @Test
    public void testUpgradeBlockedByMatrix() throws Exception {
        RollingUpgradeCompatibilityMatrix matrix = RollingUpgradeCompatibilityMatrix.from(Collections.singleton(
            new RollingUpgradeCompatibilityRule("2.18.0", "2.18.1")
        ));

        GridTestUtils.assertThrowsAnyCause(log, () -> {
            RollingUpgradeCompatibilityChecker.validate(
                IgniteProductVersion.fromString("2.18.0"),
                IgniteProductVersion.fromString("2.19.0"),
                false,
                matrix,
                NullLogger.INSTANCE
            );

            return null;
        }, IgniteCheckedException.class, "Upgrade from 2.18.0 to 2.19.0 is not allowed by compatibility matrix");
    }

    /** */
    @Test
    public void testFallbackToLegacyRules() throws Exception {
        GridTestUtils.assertThrowsAnyCause(log, () -> {
            RollingUpgradeCompatibilityChecker.validate(
                IgniteProductVersion.fromString("2.18.0"),
                IgniteProductVersion.fromString("2.20.0"),
                false,
                null,
                NullLogger.INSTANCE
            );

            return null;
        }, IgniteCheckedException.class, "Minor version can only be incremented by 1");
    }
}
