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
package rain.filter.firewall;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rain.core.filterchain.IoFilter;
import rain.core.filterchain.IoFilterAdapter;
import rain.core.session.IdleStatus;
import rain.core.session.IoSession;
import rain.core.write.WriteRequest;
import rain.filter.FilterEvent;

/**
 * A {@link IoFilter} which blocks connections from blacklisted remote
 * address.
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 * @org.apache.xbean.XBean
 */
public class BlacklistFilter extends IoFilterAdapter {
    /** The list of blocked addresses */
    private final List<Subnet> blacklist = new CopyOnWriteArrayList<Subnet>();

    /** A logger for this class */ 
    private final static Logger LOGGER = LoggerFactory.getLogger(BlacklistFilter.class);

    /**
     * Sets the addresses to be blacklisted.
     *
     * NOTE: this call will remove any previously blacklisted addresses.
     *
     * @param addresses an array of addresses to be blacklisted.
     */
    public void setBlacklist(InetAddress[] addresses) {
        if (addresses == null) {
            throw new IllegalArgumentException("addresses");
        }

        blacklist.clear();

        for (InetAddress address:addresses) {
            block(address);
        }
    }

    /**
     * Sets the subnets to be blacklisted.
     *
     * NOTE: this call will remove any previously blacklisted subnets.
     *
     * @param subnets an array of subnets to be blacklisted.
     */
    public void setSubnetBlacklist(Subnet[] subnets) {
        if (subnets == null) {
            throw new IllegalArgumentException("Subnets must not be null");
        }

        blacklist.clear();

        for (Subnet subnet : subnets) {
            block(subnet);
        }
    }

    /**
     * Sets the addresses to be blacklisted.
     *
     * NOTE: this call will remove any previously blacklisted addresses.
     *
     * @param addresses a collection of InetAddress objects representing the
     *        addresses to be blacklisted.
     * @throws IllegalArgumentException if the specified collections contains
     *         non-{@link InetAddress} objects.
     */
    public void setBlacklist(Iterable<InetAddress> addresses) {
        if (addresses == null) {
            throw new IllegalArgumentException("addresses");
        }

        blacklist.clear();

        for (InetAddress address : addresses) {
            block(address);
        }
    }

    /**
     * Sets the subnets to be blacklisted.
     *
     * NOTE: this call will remove any previously blacklisted subnets.
     *
     * @param subnets an array of subnets to be blacklisted.
     */
    public void setSubnetBlacklist(Iterable<Subnet> subnets) {
        if (subnets == null) {
            throw new IllegalArgumentException("Subnets must not be null");
        }

        blacklist.clear();

        for (Subnet subnet : subnets) {
            block(subnet);
        }
    }

    /**
     * Blocks the specified endpoint.
     * 
     * @param address The address to block
     */
    public void block(InetAddress address) {
        if (address == null) {
            throw new IllegalArgumentException("Adress to block can not be null");
        }

        block(new Subnet(address, 32));
    }

    /**
     * Blocks the specified subnet.
     * 
     * @param subnet The subnet to block
     */
    public void block(Subnet subnet) {
        if (subnet == null) {
            throw new IllegalArgumentException("Subnet can not be null");
        }

        blacklist.add(subnet);
    }

    /**
     * Unblocks the specified endpoint.
     * 
     * @param address The address to unblock
     */
    public void unblock(InetAddress address) {
        if (address == null) {
            throw new IllegalArgumentException("Adress to unblock can not be null");
        }

        unblock(new Subnet(address, 32));
    }

    /**
     * Unblocks the specified subnet.
     * 
     * @param subnet The subnet to unblock
     */
    public void unblock(Subnet subnet) {
        if (subnet == null) {
            throw new IllegalArgumentException("Subnet can not be null");
        }

        blacklist.remove(subnet);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void event(NextFilter nextFilter, IoSession session, FilterEvent event) throws Exception {
        if (!isBlocked(session)) {
            // forward if not blocked
            nextFilter.event(session, event);
        } else {
            blockSession(session);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sessionCreated(NextFilter nextFilter, IoSession session) {
        if (!isBlocked(session)) {
            // forward if not blocked
            nextFilter.sessionCreated(session);
        } else {
            blockSession(session);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sessionOpened(NextFilter nextFilter, IoSession session) throws Exception {
        if (!isBlocked(session)) {
            // forward if not blocked
            nextFilter.sessionOpened(session);
        } else {
            blockSession(session);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sessionIdle(NextFilter nextFilter, IoSession session, IdleStatus status) throws Exception {
        if (!isBlocked(session)) {
            // forward if not blocked
            nextFilter.sessionIdle(session, status);
        } else {
            blockSession(session);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void messageReceived(NextFilter nextFilter, IoSession session, Object message) {
        if (!isBlocked(session)) {
            // forward if not blocked
            nextFilter.messageReceived(session, message);
        } else {
            blockSession(session);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void messageSent(NextFilter nextFilter, IoSession session, WriteRequest writeRequest) throws Exception {
        if (!isBlocked(session)) {
            // forward if not blocked
            nextFilter.messageSent(session, writeRequest);
        } else {
            blockSession(session);
        }
    }

    private void blockSession(IoSession session) {
        LOGGER.warn("Remote address in the blacklist; closing.");
        session.closeNow();
    }

    private boolean isBlocked(IoSession session) {
        SocketAddress remoteAddress = session.getRemoteAddress();

        if (remoteAddress instanceof InetSocketAddress) {
            InetAddress address = ((InetSocketAddress) remoteAddress).getAddress();

            // check all subnets
            for (Subnet subnet : blacklist) {
                if (subnet.inSubnet(address)) {
                    return true;
                }
            }
        }

        return false;
    }
}
