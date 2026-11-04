package redis.dr.service;

import java.io.IOException;

/** Cluster mode: checks that A and B have the same shard count and slot ranges. */
public interface LayoutValidator {
    /**
     * @return null when A and B have the same layout, else a description of the difference
     * @throws IOException when a cluster cannot be reached
     */
    String problem() throws IOException;
}
