package redis.dr.client;

import redis.dr.exception.RespError;
import redis.dr.util.ReplicationInfoParser;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Minimal blocking RESP client for Sentinel queries, INFO and writes to the DR master. */
public final class RespClient implements Closeable {

    private final Socket socket = new Socket();
    private final InputStream in;
    private final OutputStream out;

    public RespClient(String host, int port) throws IOException {
        socket.connect(new InetSocketAddress(host, port), 3000);
        socket.setSoTimeout(10000);
        in = new BufferedInputStream(socket.getInputStream());
        out = socket.getOutputStream();
    }

    /** Sends one command ({@code byte[]} or any object rendered with {@code String.valueOf}) and reads the reply. */
    public Object command(Object... args) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(("*" + args.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
        for (Object arg : args) {
            byte[] bytes = arg instanceof byte[] ? (byte[]) arg : String.valueOf(arg).getBytes(StandardCharsets.UTF_8);
            buf.write(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
            buf.write(bytes);
            buf.write("\r\n".getBytes(StandardCharsets.US_ASCII));
        }
        out.write(buf.toByteArray());
        out.flush();
        return read();
    }

    /** Opens a short-lived connection, sends one command and closes it. */
    public static Object once(String host, int port, Object... args) throws IOException {
        try (RespClient client = new RespClient(host, port)) {
            return client.command(args);
        }
    }

    /** {@code INFO replication} of a node, as a field map. */
    public static Map<String, String> infoReplication(String host, int port) throws IOException {
        return ReplicationInfoParser.parse((String) once(host, port, "info", "replication"));
    }

    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\r') {
                in.read();
                return sb.toString();
            }
            sb.append((char) c);
        }
        throw new EOFException("connection closed");
    }

    private Object read() throws IOException {
        String line = readLine();
        String rest = line.substring(1);
        switch (line.charAt(0)) {
            case '+':
                return rest;
            case '-':
                throw new RespError(rest);
            case ':':
                return Long.parseLong(rest);
            case '$': {
                int n = Integer.parseInt(rest);
                if (n < 0) return null;
                byte[] bytes = in.readNBytes(n + 2);
                return new String(bytes, 0, n, StandardCharsets.UTF_8);
            }
            case '*': {
                int n = Integer.parseInt(rest);
                if (n < 0) return null;
                List<Object> items = new ArrayList<>();
                for (int i = 0; i < n; i++) items.add(read());
                return items;
            }
            default:
                throw new IOException("unexpected reply: " + line);
        }
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // nothing to do
        }
    }
}
