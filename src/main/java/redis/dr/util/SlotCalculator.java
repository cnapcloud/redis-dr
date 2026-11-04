package redis.dr.util;

import java.nio.charset.StandardCharsets;

/** Key to cluster slot mapping: CRC16/XMODEM with hash tags. */
public final class SlotCalculator {
    public static final int SLOTS = 16384;

    private SlotCalculator() {
    }

    public static int slot(String key) {
        byte[] b = key.getBytes(StandardCharsets.UTF_8);
        int from = 0, to = b.length;
        for (int i = 0; i < b.length; i++) {
            if (b[i] != '{') continue;
            for (int j = i + 1; j < b.length; j++) {
                if (b[j] == '}') {
                    if (j > i + 1) {
                        from = i + 1;
                        to = j;
                    }
                    i = b.length;
                    break;
                }
            }
            break;
        }
        int crc = 0;
        for (int i = from; i < to; i++) {
            crc ^= (b[i] & 0xff) << 8;
            for (int k = 0; k < 8; k++) crc = (crc & 0x8000) != 0 ? (crc << 1) ^ 0x1021 : crc << 1;
            crc &= 0xffff;
        }
        return crc % SLOTS;
    }
}
