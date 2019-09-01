package rain.core.session;

import rain.core.future.IoFuture;

/**
 * Defines a callback for obtaining the {@link IoSession} during
 * session initialization.
 * 
 * @param <T> The IoFuture type
 *
 */
public interface IoSessionInitializer<T extends IoFuture> {
    /**
     * Initialize a session
     * 
     * @param session The IoSession to initialize
     * @param future The IoFuture to inform when the session has been initialized
     */
    //void initializeSession(IoSession session, T future);
}
