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
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Compatibility rule for rolling upgrade procedure.
 */
public class RollingUpgradeCompatibilityRule implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Source cluster version. */
    private final String sourceVersion;

    /** Target cluster version. */
    private final String targetVersion;

    /**
     * @param sourceVersion Source cluster version.
     * @param targetVersion Target cluster version.
     */
    public RollingUpgradeCompatibilityRule(String sourceVersion, String targetVersion) {
        this.sourceVersion = sourceVersion;
        this.targetVersion = targetVersion;
    }

    /**
     * @return Source cluster version for compatibility rule.
     */
    public String sourceVersion() {
        return sourceVersion;
    }

    /**
     * @return Target cluster version for compatibility rule.
     */
    public String targetVersion() {
        return targetVersion;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(@Nullable Object o) {
        if (this == o)
            return true;

        if (!(o instanceof RollingUpgradeCompatibilityRule))
            return false;

        RollingUpgradeCompatibilityRule rule = (RollingUpgradeCompatibilityRule)o;

        return Objects.equals(sourceVersion, rule.sourceVersion) && Objects.equals(targetVersion, rule.targetVersion);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(sourceVersion, targetVersion);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "RollingUpgradeCompatibilityRule [sourceVersion=" + sourceVersion + ", targetVersion=" + targetVersion + ']';
    }
}
