package redis.dr.exception;

/** The operator request cannot be served right now: engine not started, B or A not reachable, timeout (HTTP 503). */
public class UnavailableException extends DrException {
    public UnavailableException(String message) {
        super(message);
    }

    public UnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
