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
import org.apache.ignite.lang.IgniteProductVersion;

/**
 * Default compatibility validator based on {@link IgniteProductVersion} equality.
 */
public class RollingUpgradeDefaultCompatibilityValidator implements RollingUpgradeCompatibilityValidator {
    /** {@inheritDoc} */
    @Override public boolean allows(
        RollingUpgradeCompatibilityRule rule,
        IgniteProductVersion cur,
        IgniteProductVersion target
    ) throws IgniteCheckedException {
        IgniteProductVersion sourceVer = IgniteProductVersion.fromString(rule.sourceVersion());
        IgniteProductVersion targetVer = IgniteProductVersion.fromString(rule.targetVersion());

        return cur.equals(sourceVer) && target.equals(targetVer);
    }
}
