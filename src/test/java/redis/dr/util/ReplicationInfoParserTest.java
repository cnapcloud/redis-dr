package redis.dr.util;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ReplicationInfoParserTest {
    @Test
    void parsesFieldsAndIgnoresSectionHeaders() {
        String info = "# Replication\r\nrole:slave\r\nmaster_host:10.0.0.1\r\nmaster_repl_offset:1234\r\n\r\n";
        Map<String, String> fields = ReplicationInfoParser.parse(info);
        assertThat(fields).containsEntry("role", "slave").containsEntry("master_host", "10.0.0.1")
            .containsEntry("master_repl_offset", "1234").hasSize(3);
    }

    @Test
    void valueMayContainColons() {
        assertThat(ReplicationInfoParser.parse("slave0:ip=10.0.0.2,port=6379,state=online\r\nx:a:b"))
            .containsEntry("slave0", "ip=10.0.0.2,port=6379,state=online").containsEntry("x", "a:b");
    }

    @Test
    void keyCountSumsTheKeysOfAllDbs() {
        String info = "# Keyspace\r\ndb0:keys=10,expires=2,avg_ttl=100\r\ndb3:keys=5,expires=0,avg_ttl=0\r\n";
        assertThat(ReplicationInfoParser.keyCount(info)).isEqualTo(15);
        assertThat(ReplicationInfoParser.keyCount("# Keyspace\r\n")).isZero();
    }

    @Test
    void replIdsAreAbbreviatedSafely() {
        assertThat(ReplIds.abbreviate("0123456789abcdef")).isEqualTo("01234567");
        assertThat(ReplIds.abbreviate("?")).isEqualTo("?");
        assertThat(ReplIds.abbreviate(null)).isEqualTo("-");
    }
}
