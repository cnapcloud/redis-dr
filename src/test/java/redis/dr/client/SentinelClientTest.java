package redis.dr.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Sentinel nodes are asked in order, against a stub that answers RESP like a Sentinel. */
class SentinelClientTest {
    private ServerSocket server;
    private volatile String masterReply = "*2\r\n$8\r\n10.0.0.1\r\n$4\r\n6379\r\n";

    @AfterEach
    void close() throws IOException {
        if (server != null) server.close();
    }

    /** Answers each command with a canned reply chosen by what the command contains. */
    private int startStub() throws IOException {
        server = new ServerSocket(0);
        Thread thread = new Thread(() -> {
            while (!server.isClosed()) {
                try (Socket socket = server.accept()) {
                    InputStream in = socket.getInputStream();
                    byte[] buf = new byte[1024];
                    int n = in.read(buf);
                    String command = new String(buf, 0, Math.max(n, 0), StandardCharsets.UTF_8);
                    String reply = command.contains("get-master-addr-by-name")
                        ? masterReply
                        : "*1\r\n*4\r\n$2\r\nip\r\n$8\r\n10.0.0.2\r\n$5\r\nflags\r\n$5\r\nslave\r\n";
                    socket.getOutputStream().write(reply.getBytes(StandardCharsets.UTF_8));
                    socket.getOutputStream().flush();
                } catch (IOException ignored) {
                    // stub closed or client gone
                }
            }
        }, "sentinel-stub");
        thread.setDaemon(true);
        thread.start();
        return server.getLocalPort();
    }

    /** A port nothing listens on. */
    private static int deadPort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    void usesTheFirstNodeThatAnswers() throws IOException {
        int dead = deadPort();
        int alive = startStub();
        SentinelClient client = new SentinelClient(
            List.of("127.0.0.1:" + dead, "127.0.0.1:" + alive), "mymaster", "");

        assertThat(client.masterAddress()).containsExactly("10.0.0.1", "6379");
        assertThat(client.replicas()).hasSize(1);
        assertThat(client.replicas().get(0)).containsEntry("ip", "10.0.0.2").containsEntry("flags", "slave");
    }

    @Test
    void failsWhenNoNodeAnswers() throws IOException {
        SentinelClient client = new SentinelClient(List.of("127.0.0.1:" + deadPort()), "mymaster", "");
        assertThatThrownBy(client::masterAddress).isInstanceOf(IOException.class);
    }

    @Test
    void shortHostNamesGetTheDomainSuffix() throws IOException {
        SentinelClient client = new SentinelClient(List.of("127.0.0.1:1"), "mymaster", ".invalid");
        // "redis-a-0.redis-a" has a single dot, so the suffix is appended: it cannot resolve
        assertThatThrownBy(() -> client.ipOf("redis-a-0.redis-a")).isInstanceOf(IOException.class);
        assertThat(client.ipOf("127.0.0.1")).isEqualTo("127.0.0.1");
    }

    @Test
    void anUnknownMasterNameIsAnIoErrorNotANullPointer() throws IOException {
        masterReply = "*-1\r\n"; // what Sentinel answers for a name it does not monitor
        SentinelClient client = new SentinelClient(List.of("127.0.0.1:" + startStub()), "nope", "");

        assertThatThrownBy(client::masterAddress).isInstanceOf(IOException.class).hasMessageContaining("'nope'");
    }
}
