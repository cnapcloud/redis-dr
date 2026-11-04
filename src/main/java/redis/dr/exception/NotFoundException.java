package redis.dr.exception;

/** The operator request names a partition that does not exist (HTTP 404). */
public class NotFoundException extends DrException {
    public NotFoundException(String message) {
        super(message);
    }
}
