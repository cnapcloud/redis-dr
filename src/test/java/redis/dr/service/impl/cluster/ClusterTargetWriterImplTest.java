package redis.dr.service.impl.cluster;

import org.junit.jupiter.api.Test;
import redis.dr.exception.DrFatalException;
import redis.dr.model.Partition;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterTargetWriterImplTest {
    @Test
    void aDbOtherThanZeroIsFatalNotARetryableFailureOfB() {
        // fails before any network access
        ClusterTargetWriterImpl writer = new ClusterTargetWriterImpl(new Partition(0, "0-5460"), null, List.of());

        assertThatThrownBy(() -> writer.apply("r", 10, 3, "SET".getBytes(), new byte[0][]))
            .isInstanceOf(DrFatalException.class).hasMessageContaining("db 0 only");
        assertThatThrownBy(() -> writer.restore(2, "k".getBytes(), 0, new byte[0]))
            .isInstanceOf(DrFatalException.class);
    }
}
