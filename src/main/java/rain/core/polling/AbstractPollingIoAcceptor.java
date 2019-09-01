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
package rain.core.polling;

import java.net.SocketAddress;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import rain.core.RuntimeIoException;
import rain.core.filterchain.IoFilter;
import rain.core.service.AbstractIoAcceptor;
import rain.core.service.AbstractIoService;
import rain.core.service.AcceptorOperationFuture;
import rain.core.service.IoAcceptor;
import rain.core.service.IoHandler;
import rain.core.service.IoProcessor;
import rain.core.service.SimpleIoProcessorPool;
import rain.core.session.AbstractIoSession;
import rain.core.session.IoSession;
import rain.core.session.IoSessionConfig;
import rain.transport.socket.SocketSessionConfig;
import rain.transport.socket.nio.NioSocketAcceptor;
import rain.util.ExceptionMonitor;

/**
 * A base class for implementing transport using a polling strategy. The
 * underlying sockets will be checked in an active loop and woke up when an
 * socket needed to be processed. This class handle the logic behind binding,
 * accepting and disposing the server sockets. An {@link Executor} will be used
 * for running client accepting and an {@link AbstractPollingIoProcessor} will
 * be used for processing client I/O operations like reading, writing and
 * closing.
 * 
 * All the low level methods for binding, accepting, closing need to be provided
 * by the subclassing implementation.
 * 
 * @see NioSocketAcceptor for a example of implementation
 * @param <H> The type of IoHandler
 * @param <S> The type of IoSession
 * 
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public abstract class AbstractPollingIoAcceptor<S extends AbstractIoSession, ServerSocketChannel> extends AbstractIoAcceptor {
    /** A lock used to protect the selector to be waked up before it's created */
    private final Semaphore lock = new Semaphore(1);

    private final IoProcessor<S> processor;

    private final boolean createdProcessor;

    private final Queue<AcceptorOperationFuture> registerQueue = new ConcurrentLinkedQueue<>();

    private final Queue<AcceptorOperationFuture> cancelQueue = new ConcurrentLinkedQueue<>();

    private final Map<SocketAddress, ServerSocketChannel> boundHandles =  new ConcurrentHashMap<SocketAddress, ServerSocketChannel>();

    private final ServiceOperationFuture disposalFuture = new ServiceOperationFuture();

    /** A flag set when the acceptor has been created and initialized */
    private volatile boolean selectable;

    /** The thread responsible of accepting incoming requests */
    private AtomicReference<Acceptor> acceptorRef = new AtomicReference<>();

    protected boolean reuseAddress = false;

    /**
     * Define the number of socket that can wait to be accepted. Default
     * to 50 (as in the SocketServer default).
     */
    protected int backlog = 50;

    protected AbstractPollingIoAcceptor(IoSessionConfig sessionConfig, Class<? extends IoProcessor<S>> processorClass) {
        this(sessionConfig, null, new SimpleIoProcessorPool<S>(processorClass), true);
    }
    protected AbstractPollingIoAcceptor(IoSessionConfig sessionConfig, Class<? extends IoProcessor<S>> processorClass,
            int processorCount) {
        this(sessionConfig, null, new SimpleIoProcessorPool<S>(processorClass, processorCount), true);
    }
    protected AbstractPollingIoAcceptor(IoSessionConfig sessionConfig, Class<? extends IoProcessor<S>> processorClass,
            int processorCount, SelectorProvider selectorProvider ) {
        this(sessionConfig, null, new SimpleIoProcessorPool<S>(processorClass, processorCount, selectorProvider), true);
    }
    protected AbstractPollingIoAcceptor(IoSessionConfig sessionConfig, IoProcessor<S> processor) {
        this(sessionConfig, null, processor, false);
    }
    protected AbstractPollingIoAcceptor(IoSessionConfig sessionConfig, Executor executor, IoProcessor<S> processor) {
        this(sessionConfig, executor, processor, false);
    }

    private AbstractPollingIoAcceptor(IoSessionConfig sessionConfig, Executor executor, IoProcessor<S> processor,
            boolean createdProcessor) {
        super(sessionConfig, executor);

        if (processor == null) {
            throw new IllegalArgumentException("processor");
        }

        this.processor = processor;
        this.createdProcessor = createdProcessor;

        try {
            // Initialize the selector
            init();

            // The selector is now ready, we can switch the
            // flag to true so that incoming connection can be accepted
            selectable = true;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeIoException("Failed to initialize.", e);
        } finally {
            if (!selectable) {
                try {
                    destroy();
                } catch (Exception e) {
                    ExceptionMonitor.getInstance().exceptionCaught(e);
                }
            }
        }
    }

    /**
     * Initialize the polling system, will be called at construction time.
     * @throws Exception any exception thrown by the underlying system calls
     */
    protected abstract void init() throws Exception;


    /**
     * Destroy the polling system, will be called when this {@link IoAcceptor}
     * implementation will be disposed.
     * @throws Exception any exception thrown by the underlying systems calls
     */
    protected abstract void destroy() throws Exception;

    /**
     * Check for acceptable connections, interrupt when at least a server is ready for accepting.
     * All the ready server socket descriptors need to be returned by {@link #selectedHandles()}
     * @return The number of sockets having got incoming client
     * @throws Exception any exception thrown by the underlying systems calls
     */
    protected abstract int select() throws Exception;

    /**
     * Interrupt the {@link #select()} method. Used when the poll set need to be modified.
     */
    protected abstract void wakeup();

    /**
     * {@link Iterator} for the set of server sockets found with acceptable incoming connections
     *  during the last {@link #select()} call.
     * @return the list of server handles ready
     */
    protected abstract Iterator<ServerSocketChannel> selectedChannels();

    /**
     * Open a server socket for a given local address.
     * @param localAddress the associated local address
     * @return the opened server socket
     * @throws Exception any exception thrown by the underlying systems calls
     */
    protected abstract ServerSocketChannel open(SocketAddress localAddress) throws Exception;

    /**
     * Get the local address associated with a given server socket
     * @param handle the server socket
     * @return the local {@link SocketAddress} associated with this handle
     * @throws Exception any exception thrown by the underlying systems calls
     */
    protected abstract SocketAddress localAddress(ServerSocketChannel handle) throws Exception;

    /**
     * Accept a client connection for a server socket and return a new {@link IoSession}
     * associated with the given {@link IoProcessor}
     * @param processor the {@link IoProcessor} to associate with the {@link IoSession}
     * @param handle the server handle
     * @return the created {@link IoSession}
     * @throws Exception any exception thrown by the underlying systems calls
     */
    protected abstract S accept(IoProcessor<S> processor, ServerSocketChannel handle) throws Exception;

    /**
     * Close a server socket.
     * @param handle the server socket
     * @throws Exception any exception thrown by the underlying systems calls
     */
    protected abstract void close(ServerSocketChannel handle) throws Exception;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void dispose0() throws Exception {
        unbind();

        startupAcceptor();
        wakeup();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final Set<SocketAddress> bindInternal(List<? extends SocketAddress> localAddresses) throws Exception {
        // Create a bind request as a Future operation. When the selector
        // have handled the registration, it will signal this future.
        AcceptorOperationFuture request = new AcceptorOperationFuture(localAddresses);

        // adds the Registration request to the queue for the Workers
        // to handle
        registerQueue.add(request);

        // creates the Acceptor instance and has the local
        // executor kick it off.
        startupAcceptor();

        // As we just started the acceptor, we have to unblock the select()
        // in order to process the bind request we just have added to the
        // registerQueue.
        try {
            lock.acquire();

            wakeup();
        } finally {
            lock.release();
        }

        // Now, we wait until this request is completed.
        request.awaitUninterruptibly();

        if (request.getException() != null) {
            throw request.getException();
        }

        // Update the local addresses.
        // setLocalAddresses() shouldn't be called from the worker thread
        // because of deadlock.
        Set<SocketAddress> newLocalAddresses = new HashSet<>();

        for (ServerSocketChannel handle : boundHandles.values()) {
            newLocalAddresses.add(localAddress(handle));
        }

        return newLocalAddresses;
    }

    /**
     * This method is called by the doBind() and doUnbind()
     * methods.  If the acceptor is null, the acceptor object will
     * be created and kicked off by the executor.  If the acceptor
     * object is null, probably already created and this class
     * is now working, then nothing will happen and the method
     * will just return.
     */
    private void startupAcceptor() throws InterruptedException {
        // If the acceptor is not ready, clear the queues
        // TODO : they should already be clean : do we have to do that ?
        if (!selectable) {
            registerQueue.clear();
            cancelQueue.clear();
        }

        // start the acceptor if not already started
        Acceptor acceptor = acceptorRef.get();

        if (acceptor == null) {
            lock.acquire();
            acceptor = new Acceptor();

            if (acceptorRef.compareAndSet(null, acceptor)) {
                executeWorker(acceptor);
            } else {
                lock.release();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected final void unbind0(List<? extends SocketAddress> localAddresses) throws Exception {
        AcceptorOperationFuture future = new AcceptorOperationFuture(localAddresses);

        cancelQueue.add(future);
        startupAcceptor();
        wakeup();

        future.awaitUninterruptibly();
        if (future.getException() != null) {
            throw future.getException();
        }
    }

    /**
     * This class is called by the startupAcceptor() method and is
     * placed into a NamePreservingRunnable class.
     * It's a thread accepting incoming connections from clients.
     * The loop is stopped when all the bound handlers are unbound.
     */
    private class Acceptor implements Runnable {
        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            assert acceptorRef.get() == this;

            int nHandles = 0;

            // Release the lock
            lock.release();

            while (selectable) {
                try {
                	System.out.println(Thread.currentThread().getName()+"accept.... selectable:"+ selectable);
                    // Process the bound sockets to this acceptor. this actually sets the selector to OP_ACCEPT, and binds to the port on which this class will listen on.
                	// We do that before the select because  the registerQueue containing the new handler is already updated at this point.
                    nHandles += registerChannels();

                    // Detect if we have some keys ready to be processed
                    // The select() will be woke up if some new connection
                    // have occurred, or if the selector has been explicitly
                    // woke up
                    // 如果没有连接， 阻塞当前线程
                    int selected = select();

                    // Now, if the number of registered handles is 0, we can
                    // quit the loop: we don't have any socket listening
                    // for incoming connection.
                    if (nHandles == 0) {
                        acceptorRef.set(null);

                        if (registerQueue.isEmpty() && cancelQueue.isEmpty()) {
                        	System.out.println(Thread.currentThread().getName()+"accept.... break:"+ selectable);
                            break;
                        }

                        if (!acceptorRef.compareAndSet(null, this)) {
                        	System.out.println(Thread.currentThread().getName()+"accept.... break:"+ selectable);
                            break;
                        }

                    }

                    if (selected > 0) {
                        // We have some connection request, let's process
                        // them here.
                    	System.out.println(Thread.currentThread().getName()+"We have some connection request, let's process");
                    	acceptChannels(selectedChannels());
                    }

                    // check to see if any cancellation request has been made.
                    nHandles -= unregisterHandles();
                } catch (ClosedSelectorException cse) {
                    // If the selector has been closed, we can exit the loop
                    ExceptionMonitor.getInstance().exceptionCaught(cse);
                    break;
                } catch (Exception e) {
                    ExceptionMonitor.getInstance().exceptionCaught(e);

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        ExceptionMonitor.getInstance().exceptionCaught(e1);
                    }
                }
            }

            // Cleanup all the processors, and shutdown the acceptor.
            if (selectable && isDisposing()) {
                selectable = false;
                try {
                    if (createdProcessor) {
                        processor.dispose();
                    }
                } finally {
                    try {
                        synchronized (disposalLock) {
                            if (isDisposing()) {
                                destroy();
                            }
                        }
                    } catch (Exception e) {
                        ExceptionMonitor.getInstance().exceptionCaught(e);
                    } finally {
                        disposalFuture.setDone();
                    }
                }
            }
        }

        /**
         * This method will process new sessions for the Worker class.  All
         * keys that have had their status updates as per the Selector.selectedKeys()
         * method will be processed here.  Only keys that are ready to accept
         * connections are handled here.
         * <p/>
         * Session objects are created by making new instances of SocketSessionImpl
         * and passing the session object to the SocketIoProcessor class.
         */
        @SuppressWarnings("unchecked")
        private void acceptChannels(Iterator<ServerSocketChannel> channel) throws Exception {
            while (channel.hasNext()) {
            	System.out.println(Thread.currentThread().getName()+"===processHandles");
            	ServerSocketChannel handle = channel.next();
            	channel.remove();

                // Associates a new created connection to a processor,
                // and get back a session
                S session = accept(processor, handle);

                if (session == null) {
                    continue;
                }
                
                initSession(session, null);

                // add the session to the SocketIoProcessor
                session.getProcessor().add(session);
            }
        }

        /**
         * Sets up the socket communications.  Sets items such as:
         * <p/>
         * Blocking
         * Reuse address
         * Receive buffer size
         * Bind to listen port
         * Registers OP_ACCEPT for selector
         */
        private int registerChannels() {
            for (;;) {
            	 System.out.println(Thread.currentThread().getName() +"-- registerHandles");
                // The register queue contains the list of services to manage
                // in this acceptor.
                AcceptorOperationFuture future = registerQueue.poll();

                if (future == null) {
                	System.out.println(Thread.currentThread().getName() +"-- registerHandles 退出");
                    return 0;
                }

                // We create a temporary map to store the bound handles,
                // as we may have to remove them all if there is an exception
                // during the sockets opening.
                Map<SocketAddress, ServerSocketChannel> channels = new ConcurrentHashMap<>();
                List<SocketAddress> localAddresses = future.getLocalAddresses();

                try {
                    // Process all the addresses
                    for (SocketAddress a : localAddresses) {
                    	ServerSocketChannel channel = open(a);
                    	channels.put(localAddress(channel), channel);
                    }

                    // Everything went ok, we can now update the map storing
                    // all the bound sockets.
                    boundHandles.putAll(channels);

                    // and notify.
                    future.setDone();
                    
                    return channels.size();
                } catch (Exception e) {
                    // We store the exception in the future
                    future.setException(e);
                } finally {
                    // Roll back if failed to bind all addresses.
                    if (future.getException() != null) {
                        for (ServerSocketChannel channel : channels.values()) {
                            try {
                                close(channel);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }

                        // Wake up the selector to be sure we will process the newly bound handle
                        // and not block forever in the select()
                        wakeup();
                    }
                }
            }
        }

        /**
         * This method just checks to see if anything has been placed into the
         * cancellation queue.  The only thing that should be in the cancelQueue
         * is CancellationRequest objects and the only place this happens is in
         * the doUnbind() method.
         */
        private int unregisterHandles() {
            int cancelledHandles = 0;
            for (;;) {
                AcceptorOperationFuture future = cancelQueue.poll();
                if (future == null) {
                    break;
                }

                // close the channels
                for (SocketAddress a : future.getLocalAddresses()) {
                	ServerSocketChannel handle = boundHandles.remove(a);

                    if (handle == null) {
                        continue;
                    }

                    try {
                        close(handle);
                        wakeup(); // wake up again to trigger thread death
                    } catch (Exception e) {
                        ExceptionMonitor.getInstance().exceptionCaught(e);
                    } finally {
                        cancelledHandles++;
                    }
                }

                future.setDone();
            }

            return cancelledHandles;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final IoSession newSession(SocketAddress remoteAddress, SocketAddress localAddress) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return the backLog
     */
    public int getBacklog() {
        return backlog;
    }

    /**
     * Sets the Backlog parameter
     * 
     * @param backlog
     *            the backlog variable
     */
    public void setBacklog(int backlog) {
        synchronized (bindLock) {
            if (isActive()) {
                throw new IllegalStateException("backlog can't be set while the acceptor is bound.");
            }

            this.backlog = backlog;
        }
    }

    /**
     * @return the flag that sets the reuseAddress information
     */
    public boolean isReuseAddress() {
        return reuseAddress;
    }

    /**
     * Set the Reuse Address flag
     * 
     * @param reuseAddress
     *            The flag to set
     */
    public void setReuseAddress(boolean reuseAddress) {
        synchronized (bindLock) {
            if (isActive()) {
                throw new IllegalStateException("backlog can't be set while the acceptor is bound.");
            }

            this.reuseAddress = reuseAddress;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SocketSessionConfig getSessionConfig() {
        return (SocketSessionConfig)sessionConfig;
    }
}
