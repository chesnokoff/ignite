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

package org.apache.ignite.internal.visor;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.compute.ComputeJobResult;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for Visor tasks intended to query data from a single node.
 */
public abstract class VisorOneNodeTask<A, R> extends VisorMultiNodeTask<A, R, R> {
    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<A> arg) {
        Collection<UUID> nodes = super.jobNodes(arg);

        assert nodes.size() == 1 : nodes;

        return nodes;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected R reduce0(List<ComputeJobResult> results) {
        assert results.size() == 1;

        ComputeJobResult res = results.get(0);

        if (res.getException() == null)
            return res.getData();

        throw res.getException();
    }
}
