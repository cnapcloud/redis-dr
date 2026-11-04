package redis.dr.util;

/** Formatting helpers for replication ids in log lines. */
public final class ReplIds {
    private ReplIds() {
    }

    /** First 8 characters of a replication id; safe for "?" and null. */
    public static String abbreviate(String replId) {
        if (replId == null) return "-";
        return replId.length() <= 8 ? replId : replId.substring(0, 8);
    }
}
