
package rain.transport.socket.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Executor;

import rain.core.polling.AbstractPollingIoAcceptor;
import rain.core.service.IoAcceptor;
import rain.core.service.IoProcessor;
import rain.core.service.IoService;
import rain.core.service.SimpleIoProcessorPool;
import rain.core.service.TransportMetadata;
import rain.transport.socket.DefaultSocketSessionConfig;
import rain.transport.socket.SocketAcceptor;

public final class NioSocketAcceptor extends AbstractPollingIoAcceptor<NioSession, ServerSocketChannel>
		implements SocketAcceptor {

	private volatile Selector selector;

	public NioSocketAcceptor() {
		super(new DefaultSocketSessionConfig(), NioProcessor.class);
		((DefaultSocketSessionConfig) getSessionConfig()).init(this);
	}

	public NioSocketAcceptor(int processorCount) {
		super(new DefaultSocketSessionConfig(), NioProcessor.class, processorCount);
		((DefaultSocketSessionConfig) getSessionConfig()).init(this);
	}

	public NioSocketAcceptor(IoProcessor<NioSession> processor) {
		super(new DefaultSocketSessionConfig(), processor);
		((DefaultSocketSessionConfig) getSessionConfig()).init(this);
	}

	public NioSocketAcceptor(Executor executor, IoProcessor<NioSession> processor) {
		super(new DefaultSocketSessionConfig(), executor, processor);
		((DefaultSocketSessionConfig) getSessionConfig()).init(this);
	}

	public NioSocketAcceptor(int processorCount, SelectorProvider selectorProvider) {
		super(new DefaultSocketSessionConfig(), NioProcessor.class, processorCount, selectorProvider);
		((DefaultSocketSessionConfig) getSessionConfig()).init(this);
	}

	@Override
	protected void init() throws Exception {
		selector = Selector.open();
	}

	@Override
	protected void destroy() throws Exception {
		if (selector != null) {
			selector.close();
		}
	}

	public TransportMetadata getTransportMetadata() {
		return NioSocketSession.METADATA;
	}

	@Override
	public InetSocketAddress getLocalAddress() {
		return (InetSocketAddress) super.getLocalAddress();
	}

	@Override
	public InetSocketAddress getDefaultLocalAddress() {
		return (InetSocketAddress) super.getDefaultLocalAddress();
	}

	public void setDefaultLocalAddress(InetSocketAddress localAddress) {
		setDefaultLocalAddress((SocketAddress) localAddress);
	}

	@Override
	protected NioSession accept(IoProcessor<NioSession> processor, ServerSocketChannel handle) throws Exception {
		SelectionKey key = null;
		if (handle != null) {
			key = handle.keyFor(selector);
		}
		if ((key == null) || (!key.isValid()) || (!key.isAcceptable())) {
			return null;
		}
		// accept the connection from the client
		try {
			SocketChannel ch = handle.accept();

			if (ch == null) {
				return null;
			}

			return new NioSocketSession(this, processor, ch);
		} catch (Throwable t) {
			if (t.getMessage().equals("Too many open files")) {
				LOGGER.error("Error Calling Accept on Socket - Sleeping Acceptor Thread. Check the ulimit parameter",
						t);
				try {
					Thread.sleep(50L);
				} catch (InterruptedException ie) {
					// Nothing to do
				}
			} else {
				throw t;
			}

			// No session when we have met an exception
			return null;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected ServerSocketChannel open(SocketAddress localAddress) throws Exception {
		// Creates the listening ServerSocket

		ServerSocketChannel channel = ServerSocketChannel.open();

		boolean success = false;

		try {
			// This is a non blocking socket channel
			channel.configureBlocking(false);

			// Configure the server socket,
			ServerSocket socket = channel.socket();

			// Set the reuseAddress flag accordingly with the setting
			socket.setReuseAddress(isReuseAddress());

			// and bind.
			try {
				socket.bind(localAddress, getBacklog());
			} catch (IOException ioe) {
				// Add some info regarding the address we try to bind to the
				// message
				String newMessage = "Error while binding on " + localAddress + "\n" + "original message : "
						+ ioe.getMessage();
				Exception e = new IOException(newMessage);
				e.initCause(ioe.getCause());

				// And close the channel
				channel.close();

				throw e;
			}

			// Register the channel within the selector for ACCEPT event
			channel.register(selector, SelectionKey.OP_ACCEPT);
			success = true;
		} finally {
			if (!success) {
				close(channel);
			}
		}
		return channel;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected SocketAddress localAddress(ServerSocketChannel handle) throws Exception {
		return handle.socket().getLocalSocketAddress();
	}

	/**
	 * Check if we have at least one key whose corresponding channels is ready for
	 * I/O operations.
	 *
	 * This method performs a blocking selection operation. It returns only after at
	 * least one channel is selected, this selector's wakeup method is invoked, or
	 * the current thread is interrupted, whichever comes first.
	 * 
	 * @return The number of keys having their ready-operation set updated
	 * @throws IOException If an I/O error occurs
	 */
	@Override
	protected int select() throws Exception {
		return selector.select();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected Iterator<ServerSocketChannel> selectedChannels() {
		return new ServerSocketChannelIterator(selector.selectedKeys());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void close(ServerSocketChannel handle) throws Exception {
		SelectionKey key = handle.keyFor(selector);

		if (key != null) {
			key.cancel();
		}

		handle.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void wakeup() {
		selector.wakeup();
	}

	/**
	 * Defines an iterator for the selected-key Set returned by the
	 * selector.selectedKeys(). It replaces the SelectionKey operator.
	 */
	private static class ServerSocketChannelIterator implements Iterator<ServerSocketChannel> {
		/** The selected-key iterator */
		private final Iterator<SelectionKey> iterator;

		/**
		 * Build a SocketChannel iterator which will return a SocketChannel instead of a
		 * SelectionKey.
		 * 
		 * @param selectedKeys The selector selected-key set
		 */
		private ServerSocketChannelIterator(Collection<SelectionKey> selectedKeys) {
			iterator = selectedKeys.iterator();
		}

		/**
		 * Tells if there are more SockectChannel left in the iterator
		 * 
		 * @return <tt>true</tt> if there is at least one more SockectChannel object to
		 *         read
		 */
		public boolean hasNext() {
			return iterator.hasNext();
		}

		/**
		 * Get the next SocketChannel in the operator we have built from the
		 * selected-key et for this selector.
		 * 
		 * @return The next SocketChannel in the iterator
		 */
		public ServerSocketChannel next() {
			SelectionKey key = iterator.next();

			if (key.isValid() && key.isAcceptable()) {
				return (ServerSocketChannel) key.channel();
			}

			return null;
		}

		/**
		 * Remove the current SocketChannel from the iterator
		 */
		public void remove() {
			iterator.remove();
		}
	}
}
