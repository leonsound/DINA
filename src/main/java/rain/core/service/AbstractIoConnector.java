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

import java.net.SocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import rain.core.future.ConnectFuture;
import rain.core.future.IoFuture;
import rain.core.future.IoFutureListener;
import rain.core.session.IdleStatus;
import rain.core.session.IoSession;
import rain.core.session.IoSessionConfig;
import rain.core.session.IoSessionInitializer;
import rain.filter.FilterEvent;

/**
 * A base implementation of {@link IoConnector}.
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public abstract class AbstractIoConnector extends AbstractIoService {
    /**
     * The minimum timeout value that is supported (in milliseconds).
     */
    private long connectTimeoutCheckInterval = 50L;

    private long connectTimeoutInMillis = 60 * 1000L; // 1 minute by default

    /** The remote address we are connected to */
    private SocketAddress defaultRemoteAddress;

    /** The local address */
    private SocketAddress defaultLocalAddress;

    /**
     * Constructor for {@link AbstractIoConnector}. You need to provide a
     * default session configuration and an {@link Executor} for handling I/O
     * events. If null {@link Executor} is provided, a default one will be
     * created using {@link Executors#newCachedThreadPool()}.
     * 
     * @see AbstractIoService#AbstractIoService(IoSessionConfig, Executor)
     * 
     * @param sessionConfig
     *            the default configuration for the managed {@link IoSession}
     * @param executor
     *            the {@link Executor} used for handling execution of I/O
     *            events. Can be <code>null</code>.
     */
    protected AbstractIoConnector(IoSessionConfig sessionConfig, Executor executor) {
        super(sessionConfig, executor);
    }

    /**
    * @return
     *  The minimum time that this connector can have for a connection
     *  timeout in milliseconds.
     */
    public long getConnectTimeoutCheckInterval() {
        return connectTimeoutCheckInterval;
    }

    /**
     * Sets the timeout for the connection check
     *  
     * @param minimumConnectTimeout The delay we wait before checking the connection
     */
    public void setConnectTimeoutCheckInterval(long minimumConnectTimeout) {
        if (getConnectTimeoutMillis() < minimumConnectTimeout) {
            this.connectTimeoutInMillis = minimumConnectTimeout;
        }

        this.connectTimeoutCheckInterval = minimumConnectTimeout;
    }

 

    /**
     * {@inheritDoc}
     */
    public final long getConnectTimeoutMillis() {
        return connectTimeoutInMillis;
    }

    

    /**
     * Sets the connect timeout value in milliseconds.
     * 
     */
    public final void setConnectTimeoutMillis(long connectTimeoutInMillis) {
        if (connectTimeoutInMillis <= connectTimeoutCheckInterval) {
            this.connectTimeoutCheckInterval = connectTimeoutInMillis;
        }
        this.connectTimeoutInMillis = connectTimeoutInMillis;
    }

  
    public SocketAddress getDefaultRemoteAddress() {
        return defaultRemoteAddress;
    }

 
    public final void setDefaultLocalAddress(SocketAddress localAddress) {
        defaultLocalAddress = localAddress;
    }

 
    public final SocketAddress getDefaultLocalAddress() {
        return defaultLocalAddress;
    }

 
    public final void setDefaultRemoteAddress(SocketAddress defaultRemoteAddress) {
        if (defaultRemoteAddress == null) {
            throw new IllegalArgumentException("defaultRemoteAddress");
        }

        if (!getTransportMetadata().getAddressType().isAssignableFrom(defaultRemoteAddress.getClass())) {
            throw new IllegalArgumentException("defaultRemoteAddress type: " + defaultRemoteAddress.getClass()
                    + " (expected: " + getTransportMetadata().getAddressType() + ")");
        }
        this.defaultRemoteAddress = defaultRemoteAddress;
    }

 
    public final ConnectFuture connect() {
        SocketAddress remoteAddress = getDefaultRemoteAddress();
        
        if (remoteAddress == null) {
            throw new IllegalStateException("defaultRemoteAddress is not set.");
        }

        return connect(remoteAddress, null, null);
    }

 
    public ConnectFuture connect(IoSessionInitializer<? extends ConnectFuture> sessionInitializer) {
        SocketAddress remoteAddress = getDefaultRemoteAddress();
        
        if (remoteAddress == null) {
            throw new IllegalStateException("defaultRemoteAddress is not set.");
        }

        return connect(remoteAddress, null, sessionInitializer);
    }

 
    public final ConnectFuture connect(SocketAddress remoteAddress) {
        return connect(remoteAddress, null, null);
    }

  
    public ConnectFuture connect(SocketAddress remoteAddress,
            IoSessionInitializer<? extends ConnectFuture> sessionInitializer) {
        return connect(remoteAddress, null, sessionInitializer);
    }

 
    public ConnectFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        return connect(remoteAddress, localAddress, null);
    }

 
    public final ConnectFuture connect(SocketAddress remoteAddress, SocketAddress localAddress,
            IoSessionInitializer<? extends ConnectFuture> sessionInitializer) {
        if (isDisposing()) {
            throw new IllegalStateException("The connector is being disposed.");
        }

        if (remoteAddress == null) {
            throw new IllegalArgumentException("remoteAddress");
        }

        if (!getTransportMetadata().getAddressType().isAssignableFrom(remoteAddress.getClass())) {
            throw new IllegalArgumentException("remoteAddress type: " + remoteAddress.getClass() + " (expected: "
                    + getTransportMetadata().getAddressType() + ")");
        }

        if (localAddress != null && !getTransportMetadata().getAddressType().isAssignableFrom(localAddress.getClass())) {
            throw new IllegalArgumentException("localAddress type: " + localAddress.getClass() + " (expected: "
                    + getTransportMetadata().getAddressType() + ")");
        }

        if (getHandler() == null) {
            if (getSessionConfig().isUseReadOperation()) {
                setHandler(new IoHandler() {
                    @Override
                    public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
                        // Empty handler
                    }
                    @Override
                    public void messageReceived(IoSession session, Object message) throws Exception {
                        // Empty handler
                    }
                    @Override
                    public void messageSent(IoSession session, Object message) throws Exception {
                        // Empty handler
                    }
                    @Override
                    public void sessionClosed(IoSession session) throws Exception {
                        // Empty handler
                    }

                    @Override
                    public void sessionCreated(IoSession session) throws Exception {
                        // Empty handler
                    }

                    @Override
                    public void sessionIdle(IoSession session, IdleStatus status) throws Exception {
                        // Empty handler
                    }
                    @Override
                    public void sessionOpened(IoSession session) throws Exception {
                        // Empty handler
                    }

                    @Override
                    public void inputClosed(IoSession session) throws Exception {
                        // Empty handler
                    }
                    @Override
                    public void event(IoSession session, FilterEvent event) throws Exception {
                        // Empty handler
                    }
                });
            } else {
                throw new IllegalStateException("handler is not set.");
            }
        }

        return connect0(remoteAddress, localAddress, sessionInitializer);
    }

    /**
     * Implement this method to perform the actual connect operation.
     *
     * @param remoteAddress The remote address to connect from
     * @param localAddress <tt>null</tt> if no local address is specified
     * @param sessionInitializer The IoSessionInitializer to use when the connection s successful
     * @return The ConnectFuture associated with this asynchronous operation
     * 
     */
    protected abstract ConnectFuture connect0(SocketAddress remoteAddress, SocketAddress localAddress,
            IoSessionInitializer<? extends ConnectFuture> sessionInitializer);

    /**
     * Adds required internal attributes and {@link IoFutureListener}s
     * related with event notifications to the specified {@code session}
     * and {@code future}.  Do not call this method directly;
     */
    @Override
    protected final void finishSessionInitialization0(final IoSession session, IoFuture future) {
        // In case that ConnectFuture.cancel() is invoked before
        // setSession() is invoked, add a listener that closes the
        // connection immediately on cancellation.
        future.addListener(new IoFutureListener<ConnectFuture>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public void operationComplete(ConnectFuture future) {
                if (future.isCanceled()) {
                    session.closeNow();
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        TransportMetadata m = getTransportMetadata();
        return '(' + m.getProviderName() + ' ' + m.getName() + " connector: " + "managedSessionCount: "
        + getManagedSessionCount() + ')';
    }
}
