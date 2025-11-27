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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Compatibility matrix with configurable rules.
 */
public class RollingUpgradeCompatibilityMatrix implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Registered validator. */
    private static final AtomicReference<RollingUpgradeCompatibilityValidator> validatorRef = new AtomicReference<>(
        new RollingUpgradeDefaultCompatibilityValidator()
    );

    /** Compatibility rules. */
    private final List<RollingUpgradeCompatibilityRule> rules;

    /**
     * @param rules Compatibility rules.
     */
    public RollingUpgradeCompatibilityMatrix(Collection<RollingUpgradeCompatibilityRule> rules) {
        Collection<RollingUpgradeCompatibilityRule> safeRules = rules == null ? Collections.emptyList() : rules;

        this.rules = Collections.unmodifiableList(new ArrayList<>(safeRules));
    }

    /**
     * @return Rules in this matrix.
     */
    public List<RollingUpgradeCompatibilityRule> rules() {
        return rules;
    }

    /**
     * Checks whether provided versions are allowed by this matrix.
     *
     * @param cur Current version.
     * @param target Target version.
     * @return {@code true} if upgrade is allowed by matrix.
     * @throws IgniteCheckedException If validator fails.
     */
    public boolean allows(IgniteProductVersion cur, IgniteProductVersion target) throws IgniteCheckedException {
        RollingUpgradeCompatibilityValidator validator = validatorRef.get();

        for (RollingUpgradeCompatibilityRule rule : rules) {
            if (validator.allows(rule, cur, target))
                return true;
        }

        return false;
    }

    /**
     * Registers custom compatibility validator.
     *
     * @param validator Validator to be used. When {@code null}, the default validator is restored.
     */
    public static void registerValidator(@Nullable RollingUpgradeCompatibilityValidator validator) {
        validatorRef.set(validator == null ? new RollingUpgradeDefaultCompatibilityValidator() : validator);
    }

    /**
     * Creates matrix instance from collection of rules.
     *
     * @param rules Rules collection.
     * @return Matrix instance.
     */
    public static RollingUpgradeCompatibilityMatrix from(Collection<RollingUpgradeCompatibilityRule> rules) {
        return new RollingUpgradeCompatibilityMatrix(rules);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(@Nullable Object o) {
        if (this == o)
            return true;

        if (!(o instanceof RollingUpgradeCompatibilityMatrix))
            return false;

        RollingUpgradeCompatibilityMatrix matrix = (RollingUpgradeCompatibilityMatrix)o;

        return Objects.equals(rules, matrix.rules);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(rules);
    }
}
