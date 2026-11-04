package redis.dr.client;

import org.junit.jupiter.api.Test;
import redis.dr.model.ClusterShard;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterTopologyClientTest {
    private static List<Object> node(String id, String ip, long port, String role, String health) {
        return List.of("id", id, "port", port, "ip", ip, "role", role, "health", health);
    }

    private static List<Object> shard(List<Object> slots, List<Object>... nodes) {
        return List.of("slots", slots, "nodes", List.of(nodes));
    }

    @Test
    @SuppressWarnings("unchecked")
    void parsesShardsSortedByFirstSlot() {
        List<Object> reply = List.of(
            shard(List.of(5461L, 10922L), node("b1", "10.0.1.1", 6379, "master", "online")),
            shard(List.of(0L, 5460L), node("a1", "10.0.0.1", 6379, "master", "online"),
                node("a2", "10.0.0.2", 6379, "replica", "online"),
                node("a3", "10.0.0.3", 6379, "replica", "loading")),
            shard(List.of(), node("x", "10.0.9.9", 6379, "master", "online")));

        List<ClusterShard> shards = ClusterTopologyClient.parse(reply);

        assertThat(shards).hasSize(2); // the shard without slots is left out
        assertThat(shards.get(0).slotKey()).isEqualTo("0-5460");
        assertThat(shards.get(1).slotKey()).isEqualTo("5461-10922");
        assertThat(shards.get(0).master().ip()).isEqualTo("10.0.0.1");
        assertThat(shards.get(0).replicas()).extracting("ip").containsExactly("10.0.0.2"); // loading replica excluded
        assertThat(shards.get(0).owns(5460)).isTrue();
        assertThat(shards.get(0).owns(5461)).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    void masterIsNullWhileItIsDown() {
        List<ClusterShard> shards = ClusterTopologyClient.parse(List.of(
            shard(List.of(0L, 16383L), node("a1", "10.0.0.1", 6379, "master", "fail"),
                node("a2", "10.0.0.2", 6379, "replica", "online"))));
        assertThat(shards.get(0).master()).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void multipleRangesFormOneKey() {
        List<ClusterShard> shards = ClusterTopologyClient.parse(List.of(
            shard(List.of(0L, 100L, 200L, 300L), node("a1", "10.0.0.1", 6379, "master", "online"))));
        assertThat(shards.get(0).slotKey()).isEqualTo("0-100,200-300");
        assertThat(shards.get(0).owns(150)).isFalse();
        assertThat(shards.get(0).owns(250)).isTrue();
    }
}
