package redis.dr.exception;

/** The operator request is not allowed in the current state of the worker (HTTP 409). */
public class ConflictException extends DrException {
    public ConflictException(String message) {
        super(message);
    }
}
