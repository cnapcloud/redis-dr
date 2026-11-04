package redis.dr.exception;

/** Base type of the engine's own runtime errors. */
public class DrException extends RuntimeException {
    public DrException(String message) {
        super(message);
    }

    public DrException(String message, Throwable cause) {
        super(message, cause);
    }
}
