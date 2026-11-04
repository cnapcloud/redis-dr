package redis.dr.model;

import java.util.List;

/**
 * Current state of a partition in site A.
 *
 * @param master   the master, or null while it is down and no replica has been promoted yet (cluster only)
 * @param replicas healthy replicas, sorted by IP; candidates to read the replication stream from
 */
public record SourceView(SourceNode master, List<SourceNode> replicas) {
}
