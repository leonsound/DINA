/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package rain.core.service;

import java.lang.reflect.Constructor;
import java.nio.channels.spi.SelectorProvider;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rain.core.RuntimeIoException;
import rain.core.session.AbstractIoSession;
import rain.core.session.AttributeKey;
import rain.core.session.IoSession;
import rain.core.write.WriteRequest;
 
public class SimpleIoProcessorPool<S extends AbstractIoSession> implements IoProcessor<S> {
    /** A logger for this class */
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleIoProcessorPool.class);

    /** The default pool size, when no size is provided. */
    private static final int DEFAULT_SIZE = Runtime.getRuntime().availableProcessors() + 1;

    /** A key used to store the processor pool in the session's Attributes */
    private static final AttributeKey PROCESSOR = new AttributeKey(SimpleIoProcessorPool.class, "processor");

    /** The pool table */
    private final IoProcessor<S>[] pool;

    /** The contained  which is passed to the IoProcessor when they are created */
    private final Executor executor;

    /** A flag set to true if we had to create an executor */
    private final boolean createdExecutor;

    /** A lock to protect the disposal against concurrent calls */
    private final Object disposalLock = new Object();

    /** A flg set to true if the IoProcessor in the pool are being disposed */
    private volatile boolean disposing;

    /** A flag set to true if all the IoProcessor contained in the pool have been disposed */
    private volatile boolean disposed;

    /**
     * Creates a new instance of SimpleIoProcessorPool with a default
     * size of NbCPUs +1.
     *
     * @param processorType The type of IoProcessor to use
     */
    public SimpleIoProcessorPool(Class<? extends IoProcessor<S>> processorType) {
        this(processorType, null, DEFAULT_SIZE, null);
    }

   
    public SimpleIoProcessorPool(Class<? extends IoProcessor<S>> processorType, int size) {
        this(processorType, null, size, null);
    }

    /**
     * Creates a new instance of SimpleIoProcessorPool with a defined
     * number of IoProcessors in the pool
     *
     * @param processorType The type of IoProcessor to use
     * @param size The number of IoProcessor in the pool
     * @param selectorProvider The SelectorProvider to use
     */
    public SimpleIoProcessorPool(Class<? extends IoProcessor<S>> processorType, int size, SelectorProvider selectorProvider) {
        this(processorType, null, size, selectorProvider);
    }

    /**
     * Creates a new instance of SimpleIoProcessorPool with an executor
     *
     * @param processorType The type of IoProcessor to use
     * @param executor The {@link Executor}
     */
    public SimpleIoProcessorPool(Class<? extends IoProcessor<S>> processorType, Executor executor) {
        this(processorType, executor, DEFAULT_SIZE, null);
    }

    /**
     * Creates a new instance of SimpleIoProcessorPool with an executor
     *
     * @param processorType The type of IoProcessor to use
     * @param executor The {@link Executor}
     * @param size The number of IoProcessor in the pool
     * @param selectorProvider The SelectorProvider to used
     */
    @SuppressWarnings("unchecked")
    public SimpleIoProcessorPool(Class<? extends IoProcessor<S>> processorType, Executor executor, int size, 
            SelectorProvider selectorProvider) {
    	LOGGER.error("size:{}", size);
        if (processorType == null) {
            throw new IllegalArgumentException("processorType");
        }

        if (size <= 0) {
            throw new IllegalArgumentException("size: " + size + " (expected: positive integer)");
        }

        // Create the executor if none is provided
        createdExecutor = executor == null;

        if (createdExecutor) {
            this.executor = Executors.newCachedThreadPool();
            // Set a default reject handler
            ((ThreadPoolExecutor) this.executor).setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        } else {
            this.executor = executor;
        }

        pool = new IoProcessor[size];

        boolean success = false;
        Constructor<? extends IoProcessor<S>> processorConstructor = null;
        boolean usesExecutorArg = true;

        try {
            // We create at least one processor
            try {
                try {
                    processorConstructor = processorType.getConstructor(ExecutorService.class);
                    pool[0] = processorConstructor.newInstance(this.executor);
                } catch (NoSuchMethodException e1) {
                    // To the next step...
                    try {
                        if(selectorProvider==null) {
                            processorConstructor = processorType.getConstructor(Executor.class);
                            pool[0] = processorConstructor.newInstance(this.executor);
                        } else {
                            processorConstructor = processorType.getConstructor(Executor.class, SelectorProvider.class);
                            pool[0] = processorConstructor.newInstance(this.executor,selectorProvider);
                        }
                    } catch (NoSuchMethodException e2) {
                        // To the next step...
                        try {
                            processorConstructor = processorType.getConstructor();
                            usesExecutorArg = false;
                            pool[0] = processorConstructor.newInstance();
                        } catch (NoSuchMethodException e3) {
                            // To the next step...
                        }
                    }
                }
            } catch (RuntimeException re) {
                LOGGER.error("Cannot create an IoProcessor :{}", re.getMessage());
                throw re;
            } catch (Exception e) {
                String msg = "Failed to create a new instance of " + processorType.getName() + ":" + e.getMessage();
                LOGGER.error(msg, e);
                throw new RuntimeIoException(msg, e);
            }

            if (processorConstructor == null) {
                // Raise an exception if no proper constructor is found.
                String msg = String.valueOf(processorType) + " must have a public constructor with one "
                        + ExecutorService.class.getSimpleName() + " parameter, a public constructor with one "
                        + Executor.class.getSimpleName() + " parameter or a public default constructor.";
                LOGGER.error(msg);
                throw new IllegalArgumentException(msg);
            }

            // Constructor found now use it for all subsequent instantiations
            for (int i = 1; i < pool.length; i++) {
                try {
                    if (usesExecutorArg) {
                        if(selectorProvider==null) {
                            pool[i] = processorConstructor.newInstance(this.executor);
                        } else {
                            pool[i] = processorConstructor.newInstance(this.executor, selectorProvider);
                        }
                    } else {
                        pool[i] = processorConstructor.newInstance();
                    }
                } catch (Exception e) {
                    // Won't happen because it has been done previously
                }
            }

            success = true;
        } finally {
            if (!success) {
                dispose();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void add(S session) {
        getProcessor(session).add(session);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void flush(S session) {
        getProcessor(session).flush(session);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void write(S session, WriteRequest writeRequest) {
        getProcessor(session).write(session, writeRequest);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void remove(S session) {
        getProcessor(session).remove(session);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void updateTrafficControl(S session) {
        getProcessor(session).updateTrafficControl(session);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDisposed() {
        return disposed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isDisposing() {
        return disposing;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void dispose() {
        if (disposed) {
            return;
        }

        synchronized (disposalLock) {
            if (!disposing) {
                disposing = true;

                for (IoProcessor<S> ioProcessor : pool) {
                    if (ioProcessor == null) {
                        // Special case if the pool has not been initialized properly
                        continue;
                    }

                    if (ioProcessor.isDisposing()) {
                        continue;
                    }

                    try {
                        ioProcessor.dispose();
                    } catch (Exception e) {
                        LOGGER.warn("Failed to dispose the {} IoProcessor.", ioProcessor.getClass().getSimpleName(), e);
                    }
                }

                if (createdExecutor) {
                    ((ExecutorService) executor).shutdown();
                }
            }

            Arrays.fill(pool, null);
            disposed = true;
        }
    }

    /**
     * Find the processor associated to a session. If it hasen't be stored into
     * the session's attributes, pick a new processor and stores it.
     */
    @SuppressWarnings("unchecked")
    private IoProcessor<S> getProcessor(S session) {
        IoProcessor<S> processor = (IoProcessor<S>) session.getAttribute(PROCESSOR);

        if (processor == null) {
            if (disposed || disposing) {
                throw new IllegalStateException("A disposed processor cannot be accessed.");
            }

            processor = pool[Math.abs((int) session.getId()) % pool.length];
            LOGGER.error("processor:{}",processor);
            if (processor == null) {
                throw new IllegalStateException("A disposed processor cannot be accessed.");
            }

            session.setAttributeIfAbsent(PROCESSOR, processor);
        }

        return processor;
    }
}
