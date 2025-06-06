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

package org.apache.ignite.internal.processors.platform.compute;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.IgniteComputeHandler;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformTarget;
import org.apache.ignite.internal.processors.platform.utils.PlatformFutureUtils;
import org.apache.ignite.internal.processors.platform.utils.PlatformListenable;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.processors.task.TaskExecutionOptions;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.platform.utils.PlatformFutureUtils.getResult;

/**
 * Interop compute.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class PlatformCompute extends PlatformAbstractTarget {
    /** */
    private static final int OP_BROADCAST = 2;

    /** */
    private static final int OP_EXEC = 3;

    /** */
    private static final int OP_EXEC_ASYNC = 4;

    /** */
    private static final int OP_UNICAST = 5;

    /** */
    private static final int OP_WITH_NO_FAILOVER = 6;

    /** */
    private static final int OP_WITH_TIMEOUT = 7;

    /** */
    private static final int OP_EXEC_NATIVE = 8;

    /** */
    private static final int OP_WITH_NO_RESULT_CACHE = 9;

    /** */
    private static final int OP_WITH_EXECUTOR = 10;

    /** */
    private static final int OP_AFFINITY_CALL_PARTITION = 11;

    /** */
    private static final int OP_AFFINITY_RUN_PARTITION = 12;

    /** */
    private static final int OP_AFFINITY_CALL = 13;

    /** */
    private static final int OP_AFFINITY_RUN = 14;

    /** */
    private final ClusterGroup platformGrp;

    /** */
    private final ClusterGroup grp;

    /** */
    private final String execName;

    /** */
    private final String platformAttr;

    /** */
    private final IgniteComputeHandler compute;

    /**
     * @param platformCtx Platform context.
     * @param grp Cluster group associated with this compute instance.
     */
    public PlatformCompute(PlatformContext platformCtx, ClusterGroup grp, String platformAttr) {
        this(platformCtx, grp, platformAttr, null, null);
    }

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param grp Cluster group associated with this compute instance.
     * @param platformAttr Platform attribute.
     * @param execName Custom executor name associated with this compute instance.
     * @param compute Optional compute handler from which the initial task execution options will be copied.
     */
    private PlatformCompute(
        PlatformContext platformCtx,
        ClusterGroup grp,
        String platformAttr,
        String execName,
        @Nullable IgniteComputeHandler compute
    ) {
        super(platformCtx);

        assert grp != null;
        assert platformAttr != null;

        this.grp = grp;
        this.execName = execName;
        this.platformAttr = platformAttr;

        this.compute = compute == null
            ? new IgniteComputeHandler(platformCtx.kernalContext(), this::enrichOptions)
            : new IgniteComputeHandler(compute, this::enrichOptions);

        platformGrp = grp.forAttribute(platformAttr, platformCtx.platform());
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processInStreamOutObject(int type, BinaryReaderEx reader)
        throws IgniteCheckedException {
        switch (type) {
            case OP_UNICAST:
                return processClosures(reader.readLong(), reader, false);

            case OP_BROADCAST:
                return processClosures(reader.readLong(), reader, true);

            case OP_EXEC_NATIVE: {
                long taskPtr = reader.readLong();
                long topVer = reader.readLong();
                String taskName = reader.readString();
                boolean taskSesFullSupport = reader.readBoolean();

                final PlatformFullTask task = new PlatformFullTask(
                    platformCtx, platformGrp, taskPtr, topVer, taskName, taskSesFullSupport);

                return executeNative0(task);
            }

            case OP_EXEC_ASYNC:
                return wrapListenable((PlatformListenable)executeJavaTask(reader, true));

            case OP_WITH_EXECUTOR: {
                String executorName = reader.readString();

                return new PlatformCompute(
                    platformCtx,
                    grp,
                    platformAttr,
                    executorName,
                    compute
                );
            }

            case OP_AFFINITY_CALL_PARTITION: {
                Collection<String> cacheNames = PlatformUtils.readStrings(reader);
                int part = reader.readInt();
                Object func = reader.readObjectDetached();
                long ptr = reader.readLong();
                String funcName = reader.readString();

                PlatformCallable callable = new PlatformCallable(func, ptr, funcName);

                IgniteInternalFuture fut = compute.affinityCallAsync(cacheNames, part, callable);

                return wrapListenable(readAndListenFuture(reader, fut));
            }

            case OP_AFFINITY_CALL: {
                String cacheName = reader.readString();
                Object key = reader.readObjectDetached();
                Object func = reader.readObjectDetached();
                long ptr = reader.readLong();
                String callableName = reader.readString();

                PlatformCallable callable = new PlatformCallable(func, ptr, callableName);

                IgniteInternalFuture fut = compute.affinityCallAsync(Collections.singletonList(cacheName), key, callable);

                return wrapListenable(readAndListenFuture(reader, fut));
            }

            case OP_AFFINITY_RUN_PARTITION: {
                Collection<String> cacheNames = PlatformUtils.readStrings(reader);
                int part = reader.readInt();
                Object func = reader.readObjectDetached();
                long ptr = reader.readLong();
                String runnableName = reader.readString();

                PlatformRunnable runnable = new PlatformRunnable(func, ptr, runnableName);

                IgniteInternalFuture fut = compute.affinityRunAsync(cacheNames, part, runnable);

                return wrapListenable(readAndListenFuture(reader, fut));
            }

            case OP_AFFINITY_RUN: {
                String cacheName = reader.readString();
                Object key = reader.readObjectDetached();
                Object func = reader.readObjectDetached();
                long ptr = reader.readLong();
                String runnableName = reader.readString();

                PlatformRunnable runnable = new PlatformRunnable(func, ptr, runnableName);

                IgniteInternalFuture fut = compute.affinityRunAsync(Collections.singleton(cacheName), key, runnable);

                return wrapListenable(readAndListenFuture(reader, fut));
            }

            default:
                return super.processInStreamOutObject(type, reader);
        }
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        switch (type) {
            case OP_WITH_TIMEOUT: {
                compute.withTimeout(val);

                return TRUE;
            }

            case OP_WITH_NO_FAILOVER: {
                compute.withNoFailover();

                return TRUE;
            }

            case OP_WITH_NO_RESULT_CACHE: {
                compute.withNoResultCache();

                return TRUE;
            }
        }

        return super.processInLongOutLong(type, val);
    }

    /**
     * Process closure execution request.
     *  @param taskPtr Task pointer.
     * @param reader Reader.
     * @param broadcast broadcast flag.
     */
    private PlatformTarget processClosures(long taskPtr, BinaryReaderEx reader, boolean broadcast) {
        PlatformAbstractTask task;

        int size = reader.readInt();

        if (size == 1) {
            if (broadcast) {
                PlatformBroadcastingSingleClosureTask task0 =
                    new PlatformBroadcastingSingleClosureTask(platformCtx, taskPtr);

                task0.job(nextClosureJob(task0, reader));

                task = task0;
            }
            else {
                PlatformBalancingSingleClosureTask task0 = new PlatformBalancingSingleClosureTask(platformCtx, taskPtr);

                task0.job(nextClosureJob(task0, reader));

                task = task0;
            }
        }
        else {
            if (broadcast)
                task = new PlatformBroadcastingMultiClosureTask(platformCtx, taskPtr);
            else
                task = new PlatformBalancingMultiClosureTask(platformCtx, taskPtr);

            Collection<PlatformJob> jobs = new ArrayList<>(size);

            for (int i = 0; i < size; i++)
                jobs.add(nextClosureJob(task, reader));

            if (broadcast)
                ((PlatformBroadcastingMultiClosureTask)task).jobs(jobs);
            else
                ((PlatformBalancingMultiClosureTask)task).jobs(jobs);
        }

        return executeNative0(task);
    }

    /**
     * Read the next closure job from the reader.
     *
     * @param task Task.
     * @param reader Reader.
     * @return Closure job.
     */
    private PlatformJob nextClosureJob(PlatformAbstractTask task, BinaryReaderEx reader) {
        return platformCtx.createClosureJob(task, reader.readLong(), reader.readObjectDetached(), reader.readString());
    }

    /** {@inheritDoc} */
    @Override public void processInStreamOutStream(int type, BinaryReaderEx reader, BinaryWriterEx writer)
        throws IgniteCheckedException {
        switch (type) {
            case OP_EXEC:
                writer.writeObjectDetached(executeJavaTask(reader, false));

                break;

            default:
                super.processInStreamOutStream(type, reader, writer);
        }
    }

    /**
     * Execute task.
     *
     * @param task Task.
     * @return Target.
     */
    private PlatformTarget executeNative0(final PlatformAbstractTask task) {
        IgniteInternalFuture fut = compute.withProjection(platformGrp.nodes()).executeAsync(task, null);

        fut.listen(new IgniteInClosure<IgniteInternalFuture>() {
            private static final long serialVersionUID = 0L;

            @Override public void apply(IgniteInternalFuture fut) {
                try {
                    fut.get();

                    task.onDone(null);
                }
                catch (IgniteCheckedException e) {
                    task.onDone(e);
                }
            }
        });

        return wrapListenable(PlatformFutureUtils.getListenable(fut));
    }

    /**
     * Execute task taking arguments from the given reader.
     *
     * @param reader Reader.
     * @param async Execute asynchronously flag.
     * @return Task result.
     * @throws IgniteCheckedException On error.
     */
    protected Object executeJavaTask(BinaryReaderEx reader, boolean async) throws IgniteCheckedException {
        String taskName = reader.readString();
        boolean keepBinary = reader.readBoolean();
        Object arg = reader.readObjectDetached();

        Collection<UUID> nodeIds = readNodeIds(reader);

        if (!keepBinary && (BinaryUtils.isBinaryObjectImpl(arg) || BinaryUtils.isBinaryArray(arg)))
            arg = ((BinaryObject)arg).deserialize();

        if (nodeIds != null)
            compute.withProjection(grp.forNodeIds(nodeIds).nodes());

        IgniteInternalFuture<?> fut = compute.asPublicRequest().executeAsync(taskName, arg);

        return async ? readAndListenFuture(reader, fut) : toBinary(getResult(fut));
    }

    /**
     * Convert object to binary form.
     *
     * @param src Source object.
     * @return Result.
     */
    private Object toBinary(Object src) {
        return platformCtx.kernalContext().grid().binary().toBinary(src);
    }

    /**
     * Read node IDs.
     *
     * @param reader Reader.
     * @return Node IDs.
     */
    protected Collection<UUID> readNodeIds(BinaryReaderEx reader) {
        if (reader.readBoolean()) {
            int len = reader.readInt();

            List<UUID> res = new ArrayList<>(len);

            for (int i = 0; i < len; i++)
                res.add(reader.readUuid());

            return res;
        }
        else
            return null;
    }

    /**
     * Wraps ComputeTaskFuture as IgniteInternalFuture.
     */
    protected class ComputeConvertingFuture implements IgniteInternalFuture {
        /** */
        private final IgniteInternalFuture fut;

        /**
         * Ctor.
         *
         * @param fut Future to wrap.
         */
        public ComputeConvertingFuture(ComputeTaskFuture fut) {
            this.fut = ((IgniteFutureImpl)fut).internalFuture();
        }

        /** {@inheritDoc} */
        @Override public Object get() throws IgniteCheckedException {
            return convertResult(fut.get());
        }

        /** {@inheritDoc} */
        @Override public Object get(long timeout) throws IgniteCheckedException {
            return convertResult(fut.get(timeout));
        }

        /** {@inheritDoc} */
        @Override public Object get(long timeout, TimeUnit unit) throws IgniteCheckedException {
            return convertResult(fut.get(timeout, unit));
        }

        /** {@inheritDoc} */
        @Override public Object getUninterruptibly() throws IgniteCheckedException {
            return convertResult(fut.get());
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() throws IgniteCheckedException {
            return fut.cancel();
        }

        /** {@inheritDoc} */
        @Override public boolean isDone() {
            return fut.isDone();
        }

        /** {@inheritDoc} */
        @Override public boolean isCancelled() {
            return fut.isCancelled();
        }

        /** {@inheritDoc} */
        @Override public void listen(final IgniteInClosure lsnr) {
            fut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                private static final long serialVersionUID = 0L;

                @Override public void apply(IgniteInternalFuture fut0) {
                    lsnr.apply(ComputeConvertingFuture.this);
                }
            });
        }

        /** {@inheritDoc} */
        @Override public void listen(final IgniteRunnable lsnr) {
            listen(ignored -> lsnr.run());
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture chain(IgniteClosure doneCb) {
            throw new UnsupportedOperationException("Chain operation is not supported.");
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture chain(IgniteOutClosure doneCb) {
            throw new UnsupportedOperationException("Chain operation is not supported.");
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture chain(IgniteClosure doneCb, Executor exec) {
            throw new UnsupportedOperationException("Chain operation is not supported.");
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture chain(IgniteOutClosure doneCb, Executor exec) {
            throw new UnsupportedOperationException("Chain operation is not supported.");
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture chainCompose(IgniteClosure doneCb) {
            throw new UnsupportedOperationException("Chain compose operation is not supported.");
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture chainCompose(IgniteClosure doneCb, @Nullable Executor exec) {
            throw new UnsupportedOperationException("Chain compose operation is not supported.");
        }

        /** {@inheritDoc} */
        @Override public Throwable error() {
            return fut.error();
        }

        /** {@inheritDoc} */
        @Override public Object result() {
            return convertResult(fut.result());
        }

        /**
         * Converts future result.
         *
         * @param obj Object to convert.
         * @return Result.
         */
        protected Object convertResult(Object obj) {
            return toBinary(obj);
        }
    }

    /** Enriches specified task execution options with those that are bounded to the current compute instance. */
    private TaskExecutionOptions enrichOptions(TaskExecutionOptions opts) {
        opts.withProjection(grp.nodes());

        if (execName != null)
            opts.withExecutor(execName);

        return opts;
    }
}
