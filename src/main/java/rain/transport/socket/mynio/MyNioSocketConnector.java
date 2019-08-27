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
package rain.transport.socket.mynio;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import rain.core.filterchain.DefaultIoFilterChain;
import rain.core.future.ConnectFuture;
import rain.core.future.DefaultConnectFuture;
import rain.core.future.DefaultIoFuture;
import rain.core.future.IoFuture;
import rain.core.service.IoConnector;
import rain.core.service.IoProcessor;
import rain.core.service.IoService;
import rain.core.service.SimpleIoProcessorPool;
import rain.core.service.TransportMetadata;
import rain.core.session.AbstractIoSession;
import rain.core.session.IoSession;
import rain.core.session.IoSessionInitializationException;
import rain.core.session.IoSessionInitializer;
import rain.transport.socket.DefaultSocketSessionConfig;
import rain.transport.socket.SocketSessionConfig;
import rain.transport.socket.nio.NioProcessor;
import rain.transport.socket.nio.NioSession;
import rain.transport.socket.nio.NioSocketConnector;
import rain.transport.socket.nio.NioSocketSession;
import rain.util.ExceptionMonitor;
import rain.util.NamePreservingRunnable;

/**
 * {@link IoConnector} for socket transport (TCP/IP).
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public final class MyNioSocketConnector {

	private final Queue<ConnectionRequest> connectQueue = new ConcurrentLinkedQueue<>();
	private final Queue<ConnectionRequest> cancelQueue = new ConcurrentLinkedQueue<>();
	private final IoProcessor<NioSession> processor;
	private final boolean createdProcessor;

	private final MyServiceOperationFuture disposalFuture = new MyServiceOperationFuture();
	/**
	 * The minimum timeout value that is supported (in milliseconds).
	 */
	private long connectTimeoutCheckInterval = 50L;

	private long connectTimeoutInMillis = 60 * 1000L; // 1 minute by default

	/** The remote address we are connected to */
	private SocketAddress defaultRemoteAddress;

	/** The local address */
	private SocketAddress defaultLocalAddress;

	private volatile boolean selectable;

	/** The connector thread */
	private final AtomicReference<MyConnector> connectorRef = new AtomicReference<>();
	/**
	 * The unique number identifying the Service. It's incremented for each new
	 * IoService created.
	 */
	private static final AtomicInteger id = new AtomicInteger();

	/**
	 * The thread name built from the IoService inherited instance class name and
	 * the IoService Id
	 **/
	private final String threadName;

	/**
	 * The associated executor, responsible for handling execution of I/O events.
	 */
	private final Executor executor;

	/**
	 * A flag used to indicate that the local executor has been created inside this
	 * instance, and not passed by a caller.
	 * 
	 * If the executor is locally created, then it will be an instance of the
	 * ThreadPoolExecutor class.
	 */
	private final boolean createdExecutor;

	private volatile Selector selector;
	/**
	 * A lock object which must be acquired when related resources are destroyed.
	 */
	protected final Object disposalLock = new Object();

	private volatile boolean disposing;

	private volatile boolean disposed;

	/**
	 * Constructor for {@link NioSocketConnector} with default configuration
	 * (multiple thread model).
	 */
	public MyNioSocketConnector() {
		super(new DefaultSocketSessionConfig(), NioProcessor.class);
		((DefaultSocketSessionConfig) getSessionConfig()).init(this);
	}

	/**
	 * Constructor for {@link NioSocketConnector} with default configuration, and
	 * given number of {@link NioProcessor} for multithreading I/O operations
	 * 
	 * @param processorCount the number of processor to create and place in a
	 *                       {@link SimpleIoProcessorPool}
	 */
	public MyNioSocketConnector(int processorCount) {
		super(new DefaultSocketSessionConfig(), NioProcessor.class, processorCount);
		((DefaultSocketSessionConfig) getSessionConfig()).init(this);
	}

	/**
	 * Constructor for {@link NioSocketConnector} with default configuration but a
	 * specific {@link IoProcessor}, useful for sharing the same processor over
	 * multiple {@link IoService} of the same type.
	 * 
	 * @param processor the processor to use for managing I/O events
	 */
	public MyNioSocketConnector(IoProcessor<NioSession> processor) {
		super(new DefaultSocketSessionConfig(), processor);
		((DefaultSocketSessionConfig) getSessionConfig()).init(this);
	}

	/**
	 * Constructor for {@link NioSocketConnector} with a given {@link Executor} for
	 * handling connection events and a given {@link IoProcessor} for handling I/O
	 * events, useful for sharing the same processor and executor over multiple
	 * {@link IoService} of the same type.
	 * 
	 * @param executor  the executor for connection
	 * @param processor the processor for I/O operations
	 */
	public MyNioSocketConnector(Executor executor, IoProcessor<NioSession> processor) {
		super(new DefaultSocketSessionConfig(), executor, processor);
		((DefaultSocketSessionConfig) getSessionConfig()).init(this);
	}

	/**
	 * Constructor for {@link NioSocketConnector} with default configuration which
	 * will use a built-in thread pool executor to manage the given number of
	 * processor instances. The processor class must have a constructor that accepts
	 * ExecutorService or Executor as its single argument, or, failing that, a
	 * no-arg constructor.
	 * 
	 * @param processorClass the processor class.
	 * @param processorCount the number of processors to instantiate.
	 * @see SimpleIoProcessorPool#SimpleIoProcessorPool(Class, Executor, int,
	 *      java.nio.channels.spi.SelectorProvider)
	 * @since 2.0.0-M4
	 */
	public MyNioSocketConnector(Class<? extends IoProcessor<NioSession>> processorClass, int processorCount) {
		super(new DefaultSocketSessionConfig(), processorClass, processorCount);
	}

	/**
	 * Constructor for {@link NioSocketConnector} with default configuration with
	 * default configuration which will use a built-in thread pool executor to
	 * manage the default number of processor instances. The processor class must
	 * have a constructor that accepts ExecutorService or Executor as its single
	 * argument, or, failing that, a no-arg constructor. The default number of
	 * instances is equal to the number of processor cores in the system, plus one.
	 * 
	 * @param processorClass the processor class.
	 * @see SimpleIoProcessorPool#SimpleIoProcessorPool(Class, Executor, int,
	 *      java.nio.channels.spi.SelectorProvider)
	 * @since 2.0.0-M4
	 */
	public MyNioSocketConnector(Class<? extends IoProcessor<NioSession>> processorClass) {
		super(new DefaultSocketSessionConfig(), processorClass);
	}

	protected void init() throws Exception {
		this.selector = Selector.open();
	}

	/**
	 * {@inheritDoc}
	 */
	protected void destroy() throws Exception {
		if (selector != null) {
			selector.close();
		}
	}

	public TransportMetadata getTransportMetadata() {
		return NioSocketSession.METADATA;
	}

	public SocketSessionConfig getSessionConfig() {
		return (SocketSessionConfig) sessionConfig;
	}

	public InetSocketAddress getDefaultRemoteAddress() {
		return (InetSocketAddress) super.getDefaultRemoteAddress();
	}

	public void setDefaultRemoteAddress(InetSocketAddress defaultRemoteAddress) {
		setDefaultRemoteAddress(defaultRemoteAddress);
	}

	protected Iterator<SocketChannel> allHandles() {
		return new SocketChannelIterator(selector.keys());
	}

	protected boolean connect(SocketChannel handle, SocketAddress remoteAddress) throws Exception {
		return handle.connect(remoteAddress);
	}

	@SuppressWarnings("unchecked")
	protected ConnectionRequest getConnectionRequest(SocketChannel handle) {
		SelectionKey key = handle.keyFor(selector);

		if ((key == null) || (!key.isValid())) {
			return null;
		}

		return (ConnectionRequest) key.attachment();
	}

	protected void close(SocketChannel handle) throws Exception {
		SelectionKey key = handle.keyFor(selector);

		if (key != null) {
			key.cancel();
		}

		handle.close();
	}

	protected boolean finishConnect(SocketChannel handle) throws Exception {
		if (handle.finishConnect()) {
			SelectionKey key = handle.keyFor(selector);

			if (key != null) {
				key.cancel();
			}

			return true;
		}

		return false;
	}

	protected SocketChannel newHandle(SocketAddress localAddress) throws Exception {
		SocketChannel ch = SocketChannel.open();

		int receiveBufferSize = (getSessionConfig()).getReceiveBufferSize();

		if (receiveBufferSize > 65535) {
			ch.socket().setReceiveBufferSize(receiveBufferSize);
		}

		if (localAddress != null) {
			try {
				ch.socket().bind(localAddress);
			} catch (IOException ioe) {
				// Add some info regarding the address we try to bind to the
				// message
				String newMessage = "Error while binding on " + localAddress + "\n" + "original message : "
						+ ioe.getMessage();
				Exception e = new IOException(newMessage);
				e.initCause(ioe.getCause());

				// Preemptively close the channel
				ch.close();
				throw e;
			}
		}

		ch.configureBlocking(false);

		return ch;
	}

	protected NioSession newSession(IoProcessor<NioSession> processor, SocketChannel handle) {
		return new NioSocketSession(this, processor, handle);
	}

	protected void register(SocketChannel handle, ConnectionRequest request) throws Exception {
		handle.register(selector, SelectionKey.OP_CONNECT, request);
	}

	/**
	 * {@inheritDoc}
	 */
	protected int select(int timeout) throws Exception {
		return selector.select(timeout);
	}

	protected Iterator<SocketChannel> selectedHandles() {
		return new SocketChannelIterator(selector.selectedKeys());
	}
	protected void wakeup() {
		selector.wakeup();
	}

	private static class SocketChannelIterator implements Iterator<SocketChannel> {

		private final Iterator<SelectionKey> i;

		private SocketChannelIterator(Collection<SelectionKey> selectedKeys) {
			this.i = selectedKeys.iterator();
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean hasNext() {
			return i.hasNext();
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public SocketChannel next() {
			SelectionKey key = i.next();
			return (SocketChannel) key.channel();
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void remove() {
			i.remove();
		}
	}

	/**
	 * A {@link IoFuture} dedicated class for
	 *
	 */
	protected static class MyServiceOperationFuture extends DefaultIoFuture {
		public MyServiceOperationFuture() {
			super(null);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public final boolean isDone() {
			return getValue() == Boolean.TRUE;
		}

		public final void setDone() {
			setValue(Boolean.TRUE);
		}

		public final Exception getException() {
			if (getValue() instanceof Exception) {
				return (Exception) getValue();
			}

			return null;
		}

		public final void setException(Exception exception) {
			if (exception == null) {
				throw new IllegalArgumentException("exception");
			}

			setValue(exception);
		}
	}

	public final long getConnectTimeoutMillis() {
		return connectTimeoutInMillis;
	}

	protected final void initSession(AbstractIoSession session, IoFuture future,
			IoSessionInitializer sessionInitializer) {

		// Every property but attributeMap should be set now.
		// Now initialize the attributeMap. The reason why we initialize
		// the attributeMap at last is to make sure all session properties
		// such as remoteAddress are provided to DefaultIoSessionDataStructureFactory.
		try {
			((AbstractIoSession) session)
					.setAttributeMap(session.getService().getSessionDataStructureFactory().getAttributeMap(session));
		} catch (IoSessionInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw new IoSessionInitializationException("Failed to initialize an attributeMap.", e);
		}

		try {
			((AbstractIoSession) session).setWriteRequestQueue(
					session.getService().getSessionDataStructureFactory().getWriteRequestQueue(session));
		} catch (IoSessionInitializationException e) {
			throw e;
		} catch (Exception e) {
			throw new IoSessionInitializationException("Failed to initialize a writeRequestQueue.", e);
		}

		if ((future != null) && (future instanceof ConnectFuture)) {
			// DefaultIoFilterChain will notify the future. (We support ConnectFuture only
			// for now).
			session.setAttribute(DefaultIoFilterChain.SESSION_CREATED_FUTURE, future);
		}

		if (sessionInitializer != null) {
			sessionInitializer.initializeSession(session, future);
		}

	}

	private class MyConnector implements Runnable {
		/**
		 * {@inheritDoc}
		 */
		@Override
		public void run() {
			assert connectorRef.get() == this;

			int nHandles = 0;

			while (selectable) {
				try {
					// the timeout for select shall be smaller of the connect
					// timeout or 1 second...
					int timeout = (int) Math.min(getConnectTimeoutMillis(), 1000L);
					int selected = select(timeout);

					nHandles += registerNew();

					// get a chance to get out of the connector loop, if we
					// don't have any more handles
					if (nHandles == 0) {
						connectorRef.set(null);

						if (connectQueue.isEmpty()) {
							assert connectorRef.get() != this;
							break;
						}

						if (!connectorRef.compareAndSet(null, this)) {
							assert connectorRef.get() != this;
							break;
						}

						assert connectorRef.get() == this;
					}

					if (selected > 0) {
						nHandles -= processConnections(selectedHandles());
					}

					processTimedOutSessions(allHandles());

					nHandles -= cancelKeys();
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

			if (selectable && disposing) {
				selectable = false;
				try {
					if (createdProcessor) {
						processor.dispose();
					}
				} finally {
					try {
						synchronized (disposalLock) {
							if (disposing) {
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

		private int registerNew() {
			int nHandles = 0;
			for (;;) {
				ConnectionRequest req = connectQueue.poll();
				if (req == null) {
					break;
				}

				SocketChannel handle = req.handle;
				try {
					register(handle, req);
					nHandles++;
				} catch (Exception e) {
					req.setException(e);
					try {
						close(handle);
					} catch (Exception e2) {
						ExceptionMonitor.getInstance().exceptionCaught(e2);
					}
				}
			}
			return nHandles;
		}

		private int cancelKeys() {
			int nHandles = 0;

			for (;;) {
				ConnectionRequest req = cancelQueue.poll();

				if (req == null) {
					break;
				}

				SocketChannel handle = req.handle;

				try {
					close(handle);
				} catch (Exception e) {
					ExceptionMonitor.getInstance().exceptionCaught(e);
				} finally {
					nHandles++;
				}
			}

			if (nHandles > 0) {
				wakeup();
			}

			return nHandles;
		}

		/**
		 * Process the incoming connections, creating a new session for each valid
		 * connection.
		 */
		private int processConnections(Iterator<SocketChannel> handlers) {
			int nHandles = 0;

			// Loop on each connection request
			while (handlers.hasNext()) {
				SocketChannel handle = handlers.next();
				handlers.remove();

				ConnectionRequest connectionRequest = getConnectionRequest(handle);

				if (connectionRequest == null) {
					continue;
				}

				boolean success = false;
				try {
					if (finishConnect(handle)) {
						NioSession session = newSession(processor, handle);
						initSession(session, connectionRequest, connectionRequest.getSessionInitializer());
						// Forward the remaining process to the IoProcessor.
						session.getProcessor().add(session);
						nHandles++;
					}
					success = true;
				} catch (Exception e) {
					connectionRequest.setException(e);
				} finally {
					if (!success) {
						// The connection failed, we have to cancel it.
						cancelQueue.offer(connectionRequest);
					}
				}
			}
			return nHandles;
		}

		private void processTimedOutSessions(Iterator<SocketChannel> handles) {
			long currentTime = System.currentTimeMillis();

			while (handles.hasNext()) {
				SocketChannel handle = handles.next();
				ConnectionRequest connectionRequest = getConnectionRequest(handle);

				if ((connectionRequest != null) && (currentTime >= connectionRequest.deadline)) {
					connectionRequest.setException(new ConnectException("Connection timed out."));
					cancelQueue.offer(connectionRequest);
				}
			}
		}
	}

	/**
	 * A ConnectionRequest's Iouture
	 */
	public final class ConnectionRequest extends DefaultConnectFuture {
		/** The handle associated with this connection request */
		private final SocketChannel handle;

		/** The time up to this connection request will be valid */
		private final long deadline;

		/** The callback to call when the session is initialized */
		private final IoSessionInitializer<? extends ConnectFuture> sessionInitializer;

		/**
		 * Creates a new ConnectionRequest instance
		 * 
		 * @param handle   The IoHander
		 * @param callback The IoFuture callback
		 */
		public ConnectionRequest(SocketChannel handle, IoSessionInitializer<? extends ConnectFuture> callback) {
			this.handle = handle;
			long timeout = getConnectTimeoutMillis();

			if (timeout <= 0L) {
				this.deadline = Long.MAX_VALUE;
			} else {
				this.deadline = System.currentTimeMillis() + timeout;
			}

			this.sessionInitializer = callback;
		}

		/**
		 * @return The IoHandler instance
		 */
		public SocketChannel getHandle() {
			return handle;
		}

		/**
		 * @return The connection deadline
		 */
		public long getDeadline() {
			return deadline;
		}

		/**
		 * @return The session initializer callback
		 */
		public IoSessionInitializer<? extends ConnectFuture> getSessionInitializer() {
			return sessionInitializer;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean cancel() {
			if (!isDone()) {
				boolean justCancelled = super.cancel();

				// We haven't cancelled the request before, so add the future
				// in the cancel queue.
				if (justCancelled) {
					cancelQueue.add(this);
					startupWorker();
					wakeup();
				}
			}

			return true;
		}
	}

	private void startupWorker() {
		if (!selectable) {
			connectQueue.clear();
			cancelQueue.clear();
		}

		MyConnector connector = connectorRef.get();

		if (connector == null) {
			connector = new MyConnector();

			if (connectorRef.compareAndSet(null, connector)) {
				executeWorker(connector);
			}
		}
	}

	protected final void executeWorker(Runnable worker) {
		executeWorker(worker, null);
	}

	protected final void executeWorker(Runnable worker, String suffix) {
		String actualThreadName = threadName;
		if (suffix != null) {
			actualThreadName = actualThreadName + '-' + suffix;
		}
		executor.execute(new NamePreservingRunnable(worker, actualThreadName));
	}
}
