package redis.dr.runner;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import redis.dr.client.ClusterClient;
import redis.dr.client.ClusterTopologyClient;
import redis.dr.client.RespClient;
import redis.dr.config.DrProperties;
import redis.dr.model.ClusterNode;
import redis.dr.model.ClusterShard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Test helper for the DR clusters, run by hand: the engine does not start in this profile.
 *
 * <pre>
 *   redis-dr --spring.profiles.active=tool --redis-dr.mode=cluster &lt;command&gt;
 *
 *   load   &lt;prefix&gt; &lt;n&gt;         n x (string with TTL, hash, set, zset, list) into A
 *   write  &lt;prefix&gt; &lt;seconds&gt;  SET prefix:w:i continuously into A, prints the number of acknowledged writes
 *   verify                     compares every key of A with B (type, value, TTL) and looks for extra keys in B
 * </pre>
 */
@Component
@Profile("tool")
@ConditionalOnProperty(prefix = "redis-dr", name = "mode", havingValue = "cluster")
public class ToolRunner implements ApplicationRunner {
    private final ClusterTopologyClient topologyClient;
    private final List<String> sourceSeeds;
    private final List<String> targetSeeds;

    public ToolRunner(ClusterTopologyClient topologyClient, DrProperties properties) {
        this.topologyClient = topologyClient;
        this.sourceSeeds = properties.cluster().source().nodes();
        this.targetSeeds = properties.cluster().target().nodes();
    }

    @Override
    public void run(ApplicationArguments arguments) throws Exception {
        List<String> args = arguments.getNonOptionArgs();
        if (args.isEmpty()) {
            System.err.println("usage: load <prefix> <n> | write <prefix> <seconds> | verify");
            System.exit(64);
        }
        switch (args.get(0)) {
            case "load":
                load(args.get(1), Integer.parseInt(args.get(2)));
                break;
            case "write":
                write(args.get(1), Integer.parseInt(args.get(2)));
                break;
            case "verify":
                System.exit(verify() ? 0 : 1);
                break;
            default:
                System.err.println("unknown command " + args.get(0));
                System.exit(64);
        }
        System.exit(0);
    }

    private void load(String prefix, int n) throws IOException {
        try (ClusterClient a = new ClusterClient(topologyClient, sourceSeeds)) {
            for (int i = 0; i < n; i++) {
                String s = prefix + ":str:" + i, h = prefix + ":hash:" + i, st = prefix + ":set:" + i;
                String z = prefix + ":zset:" + i, l = prefix + ":list:" + i;
                a.command(s, "SET", s, "value-" + i, "EX", 3600 + i);
                a.command(h, "HSET", h, "f1", "v1-" + i, "f2", "v2-" + i, "f3", i);
                a.command(st, "SADD", st, "a" + i, "b" + i, "c" + i);
                a.command(z, "ZADD", z, 1, "a" + i, 2.5, "b" + i, 3, "c" + i);
                a.command(l, "RPUSH", l, "x" + i, "y" + i, "z" + i);
            }
        }
        System.out.println("loaded " + n * 5 + " keys with prefix " + prefix);
    }

    private void write(String prefix, int seconds) throws Exception {
        long end = System.currentTimeMillis() + seconds * 1000L, nextReport = System.currentTimeMillis() + 1000;
        long acked = 0, errors = 0;
        try (ClusterClient a = new ClusterClient(topologyClient, sourceSeeds)) {
            while (System.currentTimeMillis() < end) {
                String key = prefix + ":w:" + acked;
                try {
                    a.command(key, "SET", key, acked);
                    acked++;
                } catch (IOException e) {
                    errors++;
                }
                if (System.currentTimeMillis() >= nextReport) {
                    System.out.println("acked=" + acked + " errors=" + errors);
                    nextReport += 1000;
                }
                Thread.sleep(5);
            }
        }
        System.out.println("DONE acked=" + acked + " errors=" + errors + " (keys " + prefix + ":w:0.." + (acked - 1) + ")");
    }

    private interface Reader {
        Object call(String key, Object... args) throws IOException;
    }

    private static Object value(Reader c, String key) throws IOException {
        String type = (String) c.call(key, "TYPE", key);
        switch (type) {
            case "string":
                return c.call(key, "GET", key);
            case "hash": {
                List<?> kv = (List<?>) c.call(key, "HGETALL", key);
                TreeMap<String, String> m = new TreeMap<>();
                for (int i = 0; i + 1 < kv.size(); i += 2) m.put(String.valueOf(kv.get(i)), String.valueOf(kv.get(i + 1)));
                return m;
            }
            case "set": {
                TreeSet<String> s = new TreeSet<>();
                for (Object o : (List<?>) c.call(key, "SMEMBERS", key)) s.add(String.valueOf(o));
                return s;
            }
            case "zset":
                return c.call(key, "ZRANGE", key, 0, -1, "WITHSCORES");
            case "list":
                return c.call(key, "LRANGE", key, 0, -1);
            case "none":
                return null;
            default:
                return "unsupported:" + type;
        }
    }

    private static List<String> scanAll(String ip, int port) throws IOException {
        List<String> keys = new ArrayList<>();
        try (RespClient c = new RespClient(ip, port)) {
            String cursor = "0";
            do {
                List<?> reply = (List<?>) c.command("SCAN", cursor, "COUNT", 1000);
                cursor = String.valueOf(reply.get(0));
                for (Object k : (List<?>) reply.get(1)) keys.add(String.valueOf(k));
            } while (!cursor.equals("0"));
        }
        return keys;
    }

    private boolean verify() throws IOException {
        long aKeys = 0, bKeys = 0, missing = 0, mismatch = 0, extra = 0, expiring = 0;
        try (ClusterClient a = new ClusterClient(topologyClient, sourceSeeds); ClusterClient b = new ClusterClient(topologyClient, targetSeeds)) {
            for (ClusterShard shard : topologyClient.fetch(sourceSeeds)) {
                ClusterNode m = shard.master();
                for (String key : scanAll(m.ip(), m.port())) {
                    aKeys++;
                    Object av = value(a::command, key), bv = value(b::command, key);
                    if (av == null || (bv == null && expiringSoon(a, key))) {
                        expiring++; // A and B expire the same key at slightly different moments
                    } else if (bv == null) {
                        missing++;
                        if (missing <= 5) System.out.println("MISSING in B: " + key);
                    } else if (!String.valueOf(av).equals(String.valueOf(bv))) {
                        mismatch++;
                        if (mismatch <= 5) System.out.println("MISMATCH " + key + " A=" + av + " B=" + bv);
                    } else {
                        long at = (Long) a.command(key, "PTTL", key), bt = (Long) b.command(key, "PTTL", key);
                        boolean ttlOk = at > 0 && bt > 0 ? Math.abs(at - bt) <= 3000 : at == bt;
                        if (!ttlOk) {
                            mismatch++;
                            if (mismatch <= 5) System.out.println("TTL MISMATCH " + key + " A=" + at + " B=" + bt);
                        }
                    }
                }
            }
            for (ClusterShard shard : topologyClient.fetch(targetSeeds)) {
                ClusterNode m = shard.master();
                for (String key : scanAll(m.ip(), m.port())) {
                    if (key.contains(":dr:checkpoint:shard-") || key.contains(":dr:paused:shard-")) continue;
                    bKeys++;
                    if ("none".equals(a.command(key, "TYPE", key))) {
                        if (expiringSoon(b, key)) {
                            expiring++;
                            continue;
                        }
                        extra++;
                        if (extra <= 5) System.out.println("EXTRA in B: " + key);
                    }
                }
            }
        }
        // Counts are not compared: keys with a TTL keep expiring while the (minutes long) scans run.
        boolean ok = missing == 0 && mismatch == 0 && extra == 0;
        System.out.println("A keys=" + aKeys + " B keys(excluding checkpoints and pause flags)=" + bKeys + " missing=" + missing
            + " mismatch=" + mismatch + " extra=" + extra + " expiring(skipped)=" + expiring
            + " -> " + (ok ? "OK" : "DIFFERENT"));
        return ok;
    }

    /** True when the key is gone or has under two minutes left, so A and B may legitimately disagree about it. */
    private static boolean expiringSoon(ClusterClient c, String key) throws IOException {
        long ttl = (Long) c.command(key, "PTTL", key);
        return ttl == -2 || (ttl >= 0 && ttl < 120_000);
    }
}
