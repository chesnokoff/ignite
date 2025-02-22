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

package org.apache.ignite.internal.management.kill;

import org.apache.ignite.internal.ServiceMXBeanImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.plugin.security.SecurityPermissionSet;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.NO_PERMISSIONS;

/**
 * Task for cancel services with specified name.
 */
@GridInternal
public class CancelServiceTask extends VisorOneNodeTask<KillServiceCommandArg, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected CancelServiceJob job(KillServiceCommandArg arg) {
        return new CancelServiceJob(arg, debug);
    }

    /**
     * Job for cancel services with specified name.
     */
    private static class CancelServiceJob extends VisorJob<KillServiceCommandArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        protected CancelServiceJob(KillServiceCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(final KillServiceCommandArg arg) {
            new ServiceMXBeanImpl(ignite.context()).cancel(arg.name());

            return null;
        }

        /** {@inheritDoc} */
        @Override public SecurityPermissionSet requiredPermissions() {
            // This task does nothing but delegates the call to the Ignite public API.
            // Therefore, it is safe to execute task without any additional permissions check.
            return NO_PERMISSIONS;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CancelServiceJob.class, this);
        }
    }
}
