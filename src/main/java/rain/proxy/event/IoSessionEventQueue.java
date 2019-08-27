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
package rain.proxy.event;

import java.util.LinkedList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rain.proxy.handlers.socks.SocksProxyRequest;
import rain.proxy.session.ProxyIoSession;

/**
 * IoSessionEventQueue.java - Queue that contains filtered session events 
 * while handshake isn't done.
 * 
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 * @since MINA 2.0.0-M3
 */
public class IoSessionEventQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(IoSessionEventQueue.class);

    /**
     * The proxy session object.
     */
    private ProxyIoSession proxyIoSession;

    /**
     * Queue of session events which occurred before the proxy handshake had completed.
     */
    private Queue<IoSessionEvent> sessionEventsQueue = new LinkedList<>();

    /**
     * Creates a new proxyIoSession instance
     * 
     * @param proxyIoSession The proxy session instance
     */
    public IoSessionEventQueue(ProxyIoSession proxyIoSession) {
        this.proxyIoSession = proxyIoSession;
    }

    /**
     * Discard all events from the queue.
     */
    private void discardSessionQueueEvents() {
        synchronized (sessionEventsQueue) {
            // Free queue
            sessionEventsQueue.clear();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Event queue CLEARED");
            }
        }
    }

    /**
     * Event is enqueued only if necessary : 
     * - socks proxies do not need the reconnection feature so events are always 
     * forwarded for these.
     * - http proxies events will be enqueued while handshake has not been completed
     * or until connection was closed.
     * If connection was prematurely closed previous events are discarded and only the
     * session closed is delivered.  
     * 
     * @param evt the event to enqueue
     */
    public void enqueueEventIfNecessary(final IoSessionEvent evt) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("??? >> Enqueue {}", evt);
        }

        if (proxyIoSession.getRequest() instanceof SocksProxyRequest) {
            // No reconnection used
            evt.deliverEvent();
            
            return;
        }

        if (proxyIoSession.getHandler().isHandshakeComplete()) {
            evt.deliverEvent();
        } else {
            if (evt.getType() == IoSessionEventType.CLOSED) {
                if (proxyIoSession.isAuthenticationFailed()) {
                    proxyIoSession.getConnector().cancelConnectFuture();
                    discardSessionQueueEvents();
                    evt.deliverEvent();
                } else {
                    discardSessionQueueEvents();
                }
            } else if (evt.getType() == IoSessionEventType.OPENED) {
                // Enqueue event cause it will not reach IoHandler but deliver it to enable 
                // session creation.
                enqueueSessionEvent(evt);
                evt.deliverEvent();
            } else {
                enqueueSessionEvent(evt);
            }
        }
    }

    /**
     * Send any session event which were queued while waiting for handshaking to complete.
     * 
     * Please note this is an internal method. DO NOT USE it in your code.
     * 
     * @throws Exception If something went wrong while flushing the pending events
     */
    public void flushPendingSessionEvents() throws Exception {
        synchronized (sessionEventsQueue) {
            IoSessionEvent evt;

            while ((evt = sessionEventsQueue.poll()) != null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(" Flushing buffered event: {}", evt);
                }
                
                evt.deliverEvent();
            }
        }
    }

    /**
     * Enqueue an event to be delivered once handshaking is complete.
     * 
     * @param evt the session event to enqueue
     */
    private void enqueueSessionEvent(final IoSessionEvent evt) {
        synchronized (sessionEventsQueue) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Enqueuing event: {}", evt);
            }
            
            sessionEventsQueue.offer(evt);
        }
    }
}