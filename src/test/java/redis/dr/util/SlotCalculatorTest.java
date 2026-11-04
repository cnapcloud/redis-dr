package redis.dr.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SlotCalculatorTest {
    @Test
    void matchesRedisKeyslot() {
        // CLUSTER KEYSLOT foo -> 12182; CRC16/XMODEM of "123456789" is 0x31C3
        assertThat(SlotCalculator.slot("foo")).isEqualTo(12182);
        assertThat(SlotCalculator.slot("123456789")).isEqualTo(0x31C3 % 16384);
    }

    @Test
    void hashTagSelectsTheSlot() {
        assertThat(SlotCalculator.slot("{user1000}.following")).isEqualTo(SlotCalculator.slot("user1000"));
        assertThat(SlotCalculator.slot("{user1000}.followers")).isEqualTo(SlotCalculator.slot("user1000"));
    }

    @Test
    void emptyOrUnclosedTagHashesTheWholeKey() {
        assertThat(SlotCalculator.slot("foo{}{bar}")).isEqualTo(SlotCalculator.slot("foo{}{bar}"));
        assertThat(SlotCalculator.slot("foo{}{bar}")).isNotEqualTo(SlotCalculator.slot("bar"));
        assertThat(SlotCalculator.slot("{abc")).isNotEqualTo(SlotCalculator.slot("abc"));
    }

    @Test
    void slotIsInRange() {
        for (int i = 0; i < 1000; i++) {
            assertThat(SlotCalculator.slot("key:" + i)).isBetween(0, SlotCalculator.SLOTS - 1);
        }
    }
}
