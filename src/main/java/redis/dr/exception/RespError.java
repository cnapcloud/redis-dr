package redis.dr.exception;

import java.io.IOException;

/** The server answered with an error reply (the connection itself is fine). */
public final class RespError extends IOException {
    public RespError(String message) {
        super(message);
    }
}
