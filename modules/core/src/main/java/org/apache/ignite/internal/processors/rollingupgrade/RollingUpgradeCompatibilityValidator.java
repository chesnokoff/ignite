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

import java.io.Serializable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteProductVersion;

/**
 * Validator for rolling upgrade compatibility rules.
 */
public interface RollingUpgradeCompatibilityValidator extends Serializable {
    /**
     * Checks whether provided versions satisfy given rule.
     *
     * @param rule Compatibility rule.
     * @param cur Current cluster version.
     * @param target Target cluster version.
     * @return {@code true} when rule allows upgrade.
     * @throws IgniteCheckedException If validation failed.
     */
    boolean allows(RollingUpgradeCompatibilityRule rule, IgniteProductVersion cur, IgniteProductVersion target)
        throws IgniteCheckedException;
}
