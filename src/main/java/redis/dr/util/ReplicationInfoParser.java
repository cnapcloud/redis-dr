package redis.dr.util;

import java.util.HashMap;
import java.util.Map;

/** Parses the reply of {@code INFO replication} into a field map. */
public final class ReplicationInfoParser {
    private ReplicationInfoParser() {
    }

    public static Map<String, String> parse(String info) {
        Map<String, String> fields = new HashMap<>();
        for (String line : info.split("\r\n")) {
            int i = line.indexOf(':');
            if (i > 0) fields.put(line.substring(0, i), line.substring(i + 1));
        }
        return fields;
    }

    /** Total number of keys of all dbs in the reply of {@code INFO keyspace} ({@code db0:keys=3,expires=1,...}). */
    public static long keyCount(String keyspaceInfo) {
        long total = 0;
        for (String line : keyspaceInfo.split("\r\n")) {
            if (!line.startsWith("db")) continue;
            for (String field : line.substring(line.indexOf(':') + 1).split(",")) {
                if (field.startsWith("keys=")) total += Long.parseLong(field.substring(5));
            }
        }
        return total;
    }
}
