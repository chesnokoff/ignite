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

package org.apache.ignite.internal.direct;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageArrayType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionType;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageItemType;
import org.apache.ignite.plugin.extensions.communication.MessageMapType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.TEST_PROCESS;
import static org.apache.ignite.marshaller.Marshallers.jdk;
import static org.junit.Assert.assertArrayEquals;

/**
 * Messages marshalling test.
 */
public class DirectMarshallingMessagesTest extends GridCommonAbstractTest {
    /** */
    private static final int CHUNK_SIZE = 16;

    /** */
    private static final short NESTED_CONTAINERS_MSG_TYPE = Short.MAX_VALUE;

    /** Message factory. */
    private final MessageFactory msgFactory =
        new IgniteMessageFactoryImpl(new MessageFactoryProvider[] {
            new GridIoMessageFactory(jdk(), U.gridClassLoader()),
            factory -> factory.register(
                NESTED_CONTAINERS_MSG_TYPE,
                NestedContainersMessage::new,
                new NestedContainersMessageSerializer()
            )
        });

    /** */
    @Test
    public void testSingleNodeMessage() {
        SingleNodeMessage<?> srcMsg =
            new SingleNodeMessage<>(UUID.randomUUID(), TEST_PROCESS, "data", new Exception("error"));

        SingleNodeMessage<?> resMsg = doMarshalUnmarshal(srcMsg);

        assertEquals(srcMsg.type(), resMsg.type());
        assertEquals(srcMsg.processId(), resMsg.processId());
        assertEquals(srcMsg.response(), resMsg.response());
        assertEquals(srcMsg.error().getClass(), resMsg.error().getClass());
        assertEquals(srcMsg.error().getMessage(), resMsg.error().getMessage());
    }

    /** */
    @Test
    public void testNestedContainers() {
        NestedContainersMessage msg = new NestedContainersMessage();

        msg.nestedMap = Map.of(
            1, Map.of(1, 2L),
            2, Map.of(1, 2L)
        );

        msg.nestedCollection = Map.of(
            1, Arrays.asList(1),
            2, Arrays.asList(2)
        );

        msg.nestedArray = Map.of(
            1, new UUID[] { new UUID(1, 10) },
            2, new UUID[] { new UUID(2, 20) }
        );

        NestedContainersMessage resMsg = doMarshalUnmarshalChunked(msg);

        assertEquals(msg.nestedMap, resMsg.nestedMap);
        assertEquals(msg.nestedCollection, resMsg.nestedCollection);
        assertArrayEquals(msg.nestedArray.get(1), resMsg.nestedArray.get(1));
        assertArrayEquals(msg.nestedArray.get(2), resMsg.nestedArray.get(2));
    }

    /**
     * @param srcMsg Message to marshal.
     * @param <T> Message type.
     * @return Unmarshalled message.
     */
    private <T extends Message> T doMarshalUnmarshal(T srcMsg) {
        ByteBuffer buf = ByteBuffer.allocate(8 * 1024);

        boolean fullyWritten = loopBuffer(buf, 0, buf0 -> srcMsg.writeTo(buf0, new DirectMessageWriter(msgFactory)));
        assertTrue("The message was not written completely.", fullyWritten);

        buf.flip();

        byte b0 = buf.get();
        byte b1 = buf.get();

        short type = (short)((b1 & 0xFF) << 8 | b0 & 0xFF);

        assertEquals(srcMsg.directType(), type);

        T resMsg = (T)msgFactory.create(type);

        boolean fullyRead = loopBuffer(buf, buf.position(),
            buf0 -> resMsg.readFrom(buf0, new DirectMessageReader(msgFactory, null)));
        assertTrue("The message was not read completely.", fullyRead);

        return resMsg;
    }

    /**
     * @param srcMsg Message to marshal.
     * @param <T> Message type.
     * @return Unmarshalled message.
     */
    private <T extends Message> T doMarshalUnmarshalChunked(T srcMsg) {
        ByteBuffer buf = ByteBuffer.allocate(256);

        DirectMessageWriter writer = new DirectMessageWriter(msgFactory);

        boolean fullyWritten = false;

        while (!fullyWritten) {
            ByteBuffer chunk = ByteBuffer.allocate(CHUNK_SIZE);

            writer.setBuffer(chunk);

            fullyWritten = writer.writeMessage(srcMsg, false);

            chunk.flip();

            buf.put(chunk);
        }

        byte[] bytes = new byte[buf.position()];

        buf.flip();
        buf.get(bytes);

        DirectMessageReader reader = new DirectMessageReader(msgFactory, null);

        Message resMsg = null;

        int pos = 0;

        while (resMsg == null) {
            int len = Math.min(CHUNK_SIZE, bytes.length - pos);

            ByteBuffer chunk = ByteBuffer.allocate(len);

            chunk.put(bytes, pos, len);

            chunk.flip();

            reader.setBuffer(chunk);

            resMsg = reader.readMessage(false);

            pos += chunk.position();
        }

        return (T)resMsg;
    }

    /**
     * @param buf Byte buffer.
     * @param start Start position.
     * @param func Function that is sequentially executed on a different-sized part of the buffer.
     * @return {@code True} if the function returns {@code True} at least once, {@code False} otherwise.
     */
    private boolean loopBuffer(ByteBuffer buf, int start, Function<ByteBuffer, Boolean> func) {
        int pos = start;

        do {
            buf.position(start);
            buf.limit(++pos);

            if (func.apply(buf))
                return true;
        }
        while (pos < buf.capacity());

        return false;
    }

    /** */
    private static class NestedContainersMessage implements Message {
        /** */
        private Map<Integer, Map<Integer, Long>> nestedMap;

        /** */
        private Map<Integer, List<Integer>> nestedCollection;

        /** */
        private Map<Integer, UUID[]> nestedArray;

        /** {@inheritDoc} */
        @Override public short directType() {
            return NESTED_CONTAINERS_MSG_TYPE;
        }
    }

    /** */
    private static class NestedContainersMessageSerializer implements MessageSerializer<NestedContainersMessage> {
        /** */
        private static final MessageMapType NESTED_MAP_TYPE = new MessageMapType(
            new MessageItemType(MessageCollectionItemType.INT),
            new MessageMapType(
                new MessageItemType(MessageCollectionItemType.INT),
                new MessageItemType(MessageCollectionItemType.LONG),
                false
            ),
            false
        );

        /** */
        private static final MessageMapType NESTED_COLLECTION_TYPE = new MessageMapType(
            new MessageItemType(MessageCollectionItemType.INT),
            new MessageCollectionType(new MessageItemType(MessageCollectionItemType.INT), false),
            false
        );

        /** */
        private static final MessageMapType NESTED_ARRAY_TYPE = new MessageMapType(
            new MessageItemType(MessageCollectionItemType.INT),
            new MessageArrayType(new MessageItemType(MessageCollectionItemType.UUID), UUID.class),
            false
        );

        /** {@inheritDoc} */
        @Override public boolean writeTo(NestedContainersMessage msg, MessageWriter writer) {
            if (!writer.isHeaderWritten()) {
                if (!writer.writeHeader(msg.directType()))
                    return false;

                writer.onHeaderWritten();
            }

            switch (writer.state()) {
                case 0:
                    if (!writer.writeMap(msg.nestedMap, NESTED_MAP_TYPE))
                        return false;

                    writer.incrementState();

                case 1:
                    if (!writer.writeMap(msg.nestedCollection, NESTED_COLLECTION_TYPE))
                        return false;

                    writer.incrementState();

                case 2:
                    if (!writer.writeMap(msg.nestedArray, NESTED_ARRAY_TYPE))
                        return false;

                    writer.incrementState();
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(NestedContainersMessage msg, MessageReader reader) {
            switch (reader.state()) {
                case 0:
                    msg.nestedMap = reader.readMap(NESTED_MAP_TYPE);

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();

                case 1:
                    msg.nestedCollection = reader.readMap(NESTED_COLLECTION_TYPE);

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();

                case 2:
                    msg.nestedArray = reader.readMap(NESTED_ARRAY_TYPE);

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();
            }

            return true;
        }
    }
}
