package redis.dr.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NodeAddressTest {
    @Test
    void parsesHostAndPort() {
        assertThat(NodeAddress.parse("redis-a-0.redis-a:6379")).isEqualTo(new NodeAddress("redis-a-0.redis-a", 6379));
        assertThat(NodeAddress.parse(" 10.0.0.1:26379 ")).isEqualTo(new NodeAddress("10.0.0.1", 26379));
        assertThat(NodeAddress.parse("sentinel-a:26379").toString()).isEqualTo("sentinel-a:26379");
    }

    @Test
    void rejectsMalformedAddresses() {
        for (String bad : new String[]{"sentinel-a", ":6379", "host:", "host:abc", "", null}) {
            assertThatThrownBy(() -> NodeAddress.parse(bad)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host:port");
        }
    }
}
