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
package rain.transport.socket.nio;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.channels.SocketChannel;

import rain.core.RuntimeIoException;
import rain.core.buffer.IoBuffer;
import rain.core.filterchain.IoFilter;
import rain.core.filterchain.IoFilterChain;
import rain.core.service.DefaultTransportMetadata;
import rain.core.service.IoProcessor;
import rain.core.service.IoService;
import rain.core.service.TransportMetadata;
import rain.core.session.IoSession;
import rain.filter.ssl.SslFilter;
import rain.transport.socket.AbstractSocketSessionConfig;
import rain.transport.socket.SocketSessionConfig;

/**
 * An {@link IoSession} for socket transport (TCP/IP).
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public class NioSocketSession extends NioSession {
	public static final TransportMetadata METADATA = new DefaultTransportMetadata("nio", "socket", false, true,
            InetSocketAddress.class, SocketSessionConfig.class, IoBuffer.class);

    /**
     * 
     * Creates a new instance of NioSocketSession.
     *
     * @param service the associated IoService 
     * @param processor the associated IoProcessor
     * @param channel the used channel
     */
    public NioSocketSession(IoService service, IoProcessor<NioSession> processor, SocketChannel channel) {
        super(processor, service, channel);
        config = new SessionConfigImpl();
        config.setAll(service.getSessionConfig());
    }
    
    private Socket getSocket() {
        return ((SocketChannel) channel).socket();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TransportMetadata getTransportMetadata() {
        return METADATA;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SocketSessionConfig getConfig() {
        return (SocketSessionConfig) config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    SocketChannel getChannel() {
        return (SocketChannel) channel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetSocketAddress getRemoteAddress() {
        if (channel == null) {
            return null;
        }

        Socket socket = getSocket();

        if (socket == null) {
            return null;
        }

        return (InetSocketAddress) socket.getRemoteSocketAddress();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetSocketAddress getLocalAddress() {
        if (channel == null) {
            return null;
        }

        Socket socket = getSocket();

        if (socket == null) {
            return null;
        }

        return (InetSocketAddress) socket.getLocalSocketAddress();
    }

    @Override
    public InetSocketAddress getServiceAddress() {
        return (InetSocketAddress) super.getServiceAddress();
    }

    /**
     * A private class storing a copy of the IoService configuration when the IoSession
     * is created. That allows the session to have its own configuration setting, over
     * the IoService default one.
     */
    private class SessionConfigImpl extends AbstractSocketSessionConfig {
        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isKeepAlive() {
            try {
                return getSocket().getKeepAlive();
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setKeepAlive(boolean on) {
            try {
                getSocket().setKeepAlive(on);
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isOobInline() {
            try {
                return getSocket().getOOBInline();
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setOobInline(boolean on) {
            try {
                getSocket().setOOBInline(on);
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isReuseAddress() {
            try {
                return getSocket().getReuseAddress();
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setReuseAddress(boolean on) {
            try {
                getSocket().setReuseAddress(on);
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getSoLinger() {
            try {
                return getSocket().getSoLinger();
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setSoLinger(int linger) {
            try {
                if (linger < 0) {
                    getSocket().setSoLinger(false, 0);
                } else {
                    getSocket().setSoLinger(true, linger);
                }
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isTcpNoDelay() {
            if (!isConnected()) {
                return false;
            }

            try {
                return getSocket().getTcpNoDelay();
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setTcpNoDelay(boolean on) {
            try {
                getSocket().setTcpNoDelay(on);
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getTrafficClass() {
            try {
                return getSocket().getTrafficClass();
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setTrafficClass(int tc) {
            try {
                getSocket().setTrafficClass(tc);
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getSendBufferSize() {
            try {
                return getSocket().getSendBufferSize();
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setSendBufferSize(int size) {
            try {
                getSocket().setSendBufferSize(size);
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getReceiveBufferSize() {
            try {
                return getSocket().getReceiveBufferSize();
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setReceiveBufferSize(int size) {
            try {
                getSocket().setReceiveBufferSize(size);
            } catch (SocketException e) {
                throw new RuntimeIoException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final boolean isSecured() {
        // If the session does not have a SslFilter, we can return false
        IoFilterChain chain = getFilterChain();

        IoFilter sslFilter = chain.get(SslFilter.class);

        if (sslFilter != null) {
        // Get the SslHandler from the SslFilter
            return ((SslFilter)sslFilter).isSecured(this);
        } else {
            return false;
        }
    }
}
