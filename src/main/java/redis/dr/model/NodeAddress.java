package redis.dr.model;

/** A Redis or Sentinel node address written as {@code host:port}. */
public record NodeAddress(String host, int port) {
    public static NodeAddress parse(String address) {
        String trimmed = address == null ? "" : address.trim();
        int colon = trimmed.lastIndexOf(':');
        if (colon <= 0 || colon == trimmed.length() - 1) {
            throw new IllegalArgumentException("expected host:port but got '" + address + "'");
        }
        try {
            return new NodeAddress(trimmed.substring(0, colon), Integer.parseInt(trimmed.substring(colon + 1)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("expected host:port but got '" + address + "'", e);
        }
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
