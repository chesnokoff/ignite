/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.wire;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactory;
import org.apache.ignite.internal.plugin.AbstractMessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** */
public class MessageIdsWriterTest {
    /** {@code ids.json} for {@link TestMessage} registered under 42. */
    private static final String IDS = """
        {
          "42": "org.apache.ignite.internal.wire.TestMessage"
        }
        """;

    /** */
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    /** Messages that need no description. */
    private final Set<Class<?>> handWritten = new HashSet<>();

    /** A described message gets its line in {@code ids.json}. */
    @Test
    public void testDescribed() throws IOException {
        describe(TestMessage.class);

        assertEquals(IDS, writeIds());
    }

    /** The writer fails on a message without a description and names the message. */
    @Test
    public void testMissingDescription() {
        IllegalStateException e = assertThrows(IllegalStateException.class, this::writeIds);

        assertTrue(e.getMessage(), e.getMessage().contains(TestMessage.class.getName()));
    }

    /** A message listed as hand-written needs no description and still gets its line in {@code ids.json}. */
    @Test
    public void testHandWritten() throws IOException {
        handWritten.add(TestMessage.class);

        assertEquals(IDS, writeIds());
    }

    /**
     * Creates an empty description: the writer checks only that the file exists.
     *
     * @param cls Message class.
     */
    private void describe(Class<?> cls) throws IOException {
        Path msgs = Files.createDirectories(wire().resolve("messages"));

        Files.createFile(msgs.resolve(cls.getName() + ".json"));
    }

    /**
     * Runs the writer over {@link TestMessage} registered under 42.
     *
     * @return Written {@code ids.json}.
     */
    private String writeIds() throws IOException {
        MessageFactoryProvider provider = new AbstractMessageFactoryProvider() {
            @Override public void registerAll(IgniteMessageFactory factory) {
                register(factory, TestMessage.class, (short)42);
            }
        };

        new MessageIdsWriter(new MessageFactoryProvider[] {provider}, handWritten).write(wire());

        return Files.readString(wire().resolve("ids.json"));
    }

    /** @return Directory with the message descriptions: a temporary folder instead of {@code wire/}. */
    private Path wire() {
        return tmp.getRoot().toPath();
    }
}
