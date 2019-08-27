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
package rain.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rain.core.service.IoHandler;
import rain.core.service.IoHandlerAdapter;
import rain.core.session.IoSession;
import rain.proxy.handlers.socks.SocksProxyRequest;
import rain.proxy.session.ProxyIoSession;

/**
 * AbstractProxyIoHandler.java - {@link IoHandler} that intercepts events until handshake is complete.
 * 
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 * @since MINA 2.0.0-M3
 */
public abstract class AbstractProxyIoHandler extends IoHandlerAdapter {
    private final static Logger LOGGER = LoggerFactory.getLogger(AbstractProxyIoHandler.class);

    /**
     * Method called only when handshake has completed.
     * 
     * @param session the io session
     * @throws Exception If the proxy session can't be opened
     */
    public abstract void proxySessionOpened(IoSession session) throws Exception;

    /**
     * Hooked session opened event.
     * 
     * @param session the io session
     */
    @Override
    public final void sessionOpened(IoSession session) throws Exception {
        ProxyIoSession proxyIoSession = (ProxyIoSession) session.getAttribute(ProxyIoSession.PROXY_SESSION);

        if (proxyIoSession.getRequest() instanceof SocksProxyRequest || proxyIoSession.isAuthenticationFailed()
                || proxyIoSession.getHandler().isHandshakeComplete()) {
            proxySessionOpened(session);
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Filtered session opened event !");
            }
        }
    }
}