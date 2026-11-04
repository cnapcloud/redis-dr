package redis.dr.service.impl.cluster;

import org.junit.jupiter.api.Test;
import redis.dr.model.ClusterShard;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LayoutValidatorImplTest {
    private static ClusterShard shard(long from, long to) {
        return new ClusterShard(List.<long[]>of(new long[]{from, to}), List.of());
    }

    @Test
    void sameLayoutHasNoProblem() {
        assertThat(LayoutValidatorImpl.layoutProblem(
            List.of(shard(0, 8191), shard(8192, 16383)), List.of(shard(0, 8191), shard(8192, 16383)))).isNull();
    }

    @Test
    void shardCountMismatch() {
        assertThat(LayoutValidatorImpl.layoutProblem(
            List.of(shard(0, 8191), shard(8192, 16383)), List.of(shard(0, 16383))))
            .isEqualTo("shard count differs: A=2 B=1");
    }

    @Test
    void slotLayoutMismatch() {
        assertThat(LayoutValidatorImpl.layoutProblem(
            List.of(shard(0, 8191), shard(8192, 16383)), List.of(shard(0, 8000), shard(8001, 16383))))
            .isEqualTo("slot layout differs at shard 0: A=0-8191 B=0-8000");
    }
}
