package rain.core.service;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import rain.core.future.IoFuture;
import rain.core.service.AbstractIoService.ServiceOperationFuture;

/**
 * A {@link IoFuture} 
 */
public class AcceptorOperationFuture extends ServiceOperationFuture {
    private final List<SocketAddress> localAddresses;

    /**
     * Creates a new AcceptorOperationFuture instance
     * 
     * @param localAddresses The list of local addresses to listen to
     */
    public AcceptorOperationFuture(List<? extends SocketAddress> localAddresses) {
        this.localAddresses = new ArrayList<>(localAddresses);
    }

    /**
     * @return The list of local addresses we listen to
     */
    public final List<SocketAddress> getLocalAddresses() {
        return Collections.unmodifiableList(localAddresses);
    }

    /**
     * @see Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("Acceptor operation : ");

        if (localAddresses != null) {
            boolean isFirst = true;

            for (SocketAddress address : localAddresses) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    sb.append(", ");
                }

                sb.append(address);
            }
        }
        return sb.toString();
    }
}
