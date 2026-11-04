package redis.dr.exception;

/** An error the engine cannot recover from: it stops and the process exits with {@link #exitCode()}. */
public class DrFatalException extends DrException {
    /** Unexpected failure (event not applied, source commands not listable, ...). */
    public static final int EXIT_FATAL = 1;
    /** A and B have a different shard count or slot layout. */
    public static final int EXIT_LAYOUT = 2;

    private final int exitCode;

    public DrFatalException(String message, Throwable cause, int exitCode) {
        super(message, cause);
        this.exitCode = exitCode;
    }

    public DrFatalException(String message, int exitCode) {
        this(message, null, exitCode);
    }

    public int exitCode() {
        return exitCode;
    }
}
