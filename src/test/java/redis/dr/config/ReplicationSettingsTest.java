package redis.dr.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The read timeout of the replication socket against the PING period of A's Redis. */
class ReplicationSettingsTest {
    @Test
    void theDefaultsAreValid() {
        DrProperties.Replication r = new DrProperties.Replication(10, 30);
        assertThat(r.readTimeoutValid()).isTrue();
        assertThat(r.effectiveReadTimeoutSeconds()).isEqualTo(30);
    }

    @Test
    void aTimeoutMoreThanTwiceThePingPeriodIsKept() {
        assertThat(new DrProperties.Replication(10, 21).effectiveReadTimeoutSeconds()).isEqualTo(21);
        assertThat(new DrProperties.Replication(20, 45).effectiveReadTimeoutSeconds()).isEqualTo(45);
    }

    @Test
    void aTimeoutThatIsTooSmallIsReplacedByTheDefault() {
        for (int timeout : new int[] {1, 5, 10, 11, 15, 20}) { // up to and including twice the ping period
            DrProperties.Replication r = new DrProperties.Replication(10, timeout);
            assertThat(r.readTimeoutValid()).as("timeout %d", timeout).isFalse();
            assertThat(r.effectiveReadTimeoutSeconds()).as("timeout %d", timeout).isEqualTo(30);
        }
    }

    @Test
    void zeroAndNegativeTimeoutsAreNotAccepted() {
        for (int timeout : new int[] {0, -1}) {
            DrProperties.Replication r = new DrProperties.Replication(10, timeout);
            assertThat(r.readTimeoutValid()).isFalse();
            assertThat(r.effectiveReadTimeoutSeconds()).isEqualTo(30);
        }
    }

    @Test
    void whenThePingPeriodIsRaisedTheDefaultItselfMayBeTooSmall() {
        DrProperties.Replication r = new DrProperties.Replication(20, 30); // 30 is not more than 2 x 20
        assertThat(r.readTimeoutValid()).isFalse();
        assertThat(r.effectiveReadTimeoutSeconds()).isEqualTo(60); // 3 x ping, not the default 30
        assertThat(new DrProperties.Replication(20, 60).readTimeoutValid()).isTrue();
    }

    @Test
    void aPingPeriodBelowOneSecondFailsAtStartup() {
        assertThatThrownBy(() -> new DrProperties.Replication(0, 30)).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("source-ping-period-seconds");
    }
}
