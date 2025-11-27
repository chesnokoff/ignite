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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.lang.IgniteProductVersion;

/**
 * Utility methods for rolling upgrade compatibility validation.
 */
public class RollingUpgradeCompatibilityChecker {
    /**
     * Validates upgrade compatibility using provided matrix or legacy rules.
     *
     * @param cur Current cluster version.
     * @param target Target cluster version.
     * @param force {@code true} to skip compatibility checks.
     * @param matrix Compatibility matrix.
     * @param log Logger.
     * @throws IgniteCheckedException If versions are incompatible.
     */
    public static void validate(
        IgniteProductVersion cur,
        IgniteProductVersion target,
        boolean force,
        RollingUpgradeCompatibilityMatrix matrix,
        IgniteLogger log
    ) throws IgniteCheckedException {
        if (force) {
            if (log != null && log.isInfoEnabled())
                log.info("Skipping version compatibility check for rolling upgrade due to force flag "
                    + "[currentVer=" + cur + ", targetVer=" + target + ']');

            return;
        }

        if (matrix != null && !matrix.rules().isEmpty()) {
            if (!matrix.allows(cur, target))
                throw new IgniteCheckedException("Upgrade from " + cur + " to " + target
                    + " is not allowed by compatibility matrix");

            return;
        }

        if (cur.major() != target.major())
            throw new IgniteCheckedException("Major versions are different");

        if (cur.minor() != target.minor()) {
            if (target.minor() == cur.minor() + 1 && target.maintenance() == 0)
                return;

            throw new IgniteCheckedException("Minor version can only be incremented by 1");
        }

        if (cur.maintenance() + 1 != target.maintenance())
            throw new IgniteCheckedException("Patch version can only be incremented by 1");
    }
}
