package rain.core.session;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import rain.core.write.WriteRequest;

public class DefaultWriteRequestQueue {

    /** A queue to store incoming write requests */
    private final Queue<WriteRequest> q = new ConcurrentLinkedQueue<>();

    /**
     * {@inheritDoc}
     */
    public void dispose(IoSession session) {
        // Do nothing
    }

    /**
     * {@inheritDoc}
     */
    public void clear(IoSession session) {
        q.clear();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isEmpty(IoSession session) {
        return q.isEmpty();
    }
    public void offer(IoSession session, WriteRequest writeRequest) {
        q.offer(writeRequest);
    }

    /**
     * {@inheritDoc}
     */
    public WriteRequest poll(IoSession session) {
        WriteRequest answer = q.poll();

        if (answer == AbstractIoSession.CLOSE_REQUEST) {
            session.closeNow();
            dispose(session);
            answer = null;
        }

        return answer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return q.toString();
    }

    /**
     * {@inheritDoc}
     */
    public int size() {
        return q.size();
    }

}
