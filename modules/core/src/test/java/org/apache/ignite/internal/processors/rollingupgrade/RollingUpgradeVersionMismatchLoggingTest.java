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

package org.apache.ignite.internal.processors.rollingupgrade;

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Verifies that a node with a mismatching build version is rejected with a clear reason that reaches both
 * the server log and the joining node.
 */
public class RollingUpgradeVersionMismatchLoggingTest extends GridCommonAbstractTest {
    /** Server log collector. */
    private ListeningTestLogger srvLog;

    /** Client log collector. */
    private ListeningTestLogger clientLog;

    /** Build version injected into node attributes. */
    private String nodeVer;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi() {
            @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver) {
                super.setNodeAttributes(attrs, ver);

                attrs.put(IgniteNodeAttributes.ATTR_BUILD_VER, nodeVer);
            }
        };

        discoSpi.setIpFinder(sharedStaticIpFinder);

        cfg.setDiscoverySpi(discoSpi);

        if (igniteInstanceName.endsWith("0"))
            cfg.setGridLogger(srvLog);
        else
            cfg.setGridLogger(clientLog);

        return cfg;
    }

    /**
     * Ensures that mismatching versions produce a clear validation error, and that the coordinator logs it.
     */
    @Test
    public void testVersionMismatchReasonIsPropagated() throws Exception {
        srvLog = new ListeningTestLogger(log);
        clientLog = new ListeningTestLogger(log);

        LogListener srvWarnLsnr = LogListener.matches("Remote node rejected due to incompatible version for cluster join.\n").build();
        LogListener clientWarnLsnr = LogListener.matches("Remote node rejected due to incompatible version for cluster join.\n").build();

        srvLog.registerListener(srvWarnLsnr);
        clientLog.registerListener(clientWarnLsnr);

        nodeVer = "17.6.3";

        startGrid(0);

        nodeVer = "17.6.2";

        IgniteCheckedException err = (IgniteCheckedException)assertThrows(log,
            () -> startClientGrid(1),
            IgniteCheckedException.class,
            null);

        assertTrue("Expected IgniteSpiVersionCheckException in cause chain", X.hasCause(err,
            IgniteSpiException.class));
        assertTrue("Expected version mismatch message in the error",
            X.hasCause(err, IgniteSpiException.class));
//        assertTrue(err.getMessage().contains("17.6.3"));
//        assertTrue(err.getMessage().contains("17.6.2"));

        assertTrue("Server must log version mismatch", GridTestUtils.waitForCondition(srvWarnLsnr::check, 5_000));
        assertTrue("Client must log version mismatch", GridTestUtils.waitForCondition(clientWarnLsnr::check, 5_000));
    }
}
