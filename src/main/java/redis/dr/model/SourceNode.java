package redis.dr.model;

/** A Redis node of site A, by IP. */
public record SourceNode(String ip, int port) {
}
