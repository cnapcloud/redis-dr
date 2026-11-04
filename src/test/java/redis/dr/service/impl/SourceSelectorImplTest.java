package redis.dr.service.impl;

import org.junit.jupiter.api.Test;
import redis.dr.model.Checkpoint;
import redis.dr.model.Partition;
import redis.dr.model.SourceView;
import redis.dr.service.SourceSelector.Reconciliation;
import redis.dr.service.SourceTopology;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SourceSelectorImplTest {
    private static final String MASTER_IP = "10.0.0.1";
    private static final String REPLID = "a".repeat(40);
    private static final String OTHER_REPLID = "b".repeat(40);

    private final SourceSelectorImpl selector = new SourceSelectorImpl(new SourceTopology() {
        @Override
        public List<Partition> partitions() {
            return List.of();
        }

        @Override
        public SourceView view(Partition partition) {
            return null;
        }

        @Override
        public String resolveIp(String host) {
            if (host.equals("broken")) throw new IllegalStateException("cannot resolve");
            return host;
        }

        @Override
        public Map<String, String> replicationInfo(String ip, int port) {
            return Map.of();
        }
    });

    private static Checkpoint checkpoint(String replId, long offset) {
        Checkpoint cp = new Checkpoint();
        cp.set(replId, offset);
        return cp;
    }

    /** A healthy replica following MASTER_IP whose backlog covers offset 1000 under REPLID. */
    private static Map<String, String> healthyReplica() {
        Map<String, String> info = new HashMap<>();
        info.put("role", "slave");
        info.put("master_link_status", "up");
        info.put("master_host", MASTER_IP);
        info.put("master_repl_offset", "2000");
        info.put("repl_backlog_active", "1");
        info.put("repl_backlog_first_byte_offset", "500");
        info.put("master_replid", REPLID);
        info.put("master_replid2", "0".repeat(40));
        info.put("second_repl_offset", "-1");
        return info;
    }

    @Test
    void healthyReplicaThatCoversTheCheckpointIsUsable() {
        assertThat(selector.unusable(healthyReplica(), MASTER_IP, checkpoint(REPLID, 1000))).isNull();
    }

    @Test
    void anyHealthyReplicaServesAFullSync() {
        Map<String, String> info = healthyReplica();
        info.put("repl_backlog_active", "0");
        assertThat(selector.unusable(info, MASTER_IP, new Checkpoint())).isNull();
    }

    @Test
    void notAReplicaOrLinkDownOrFollowingAnotherMaster() {
        Map<String, String> info = healthyReplica();
        info.put("role", "master");
        assertThat(selector.unusable(info, MASTER_IP, checkpoint(REPLID, 1000))).isEqualTo("role=master");

        info = healthyReplica();
        info.put("master_link_status", "down");
        assertThat(selector.unusable(info, MASTER_IP, checkpoint(REPLID, 1000))).isEqualTo("link down");

        info = healthyReplica();
        info.put("master_host", "10.0.0.9");
        assertThat(selector.unusable(info, MASTER_IP, checkpoint(REPLID, 1000)))
            .isEqualTo("not following current master (10.0.0.9)");

        info = healthyReplica();
        info.put("master_host", "broken");
        assertThat(selector.unusable(info, MASTER_IP, checkpoint(REPLID, 1000))).isEqualTo("master_host unresolved");
    }

    @Test
    void replicaBehindTheCheckpoint() {
        assertThat(selector.unusable(healthyReplica(), MASTER_IP, checkpoint(REPLID, 3000))).isEqualTo("behind (2000<3000)");
    }

    @Test
    void backlogStartingAfterTheCheckpoint() {
        Map<String, String> info = healthyReplica();
        info.put("repl_backlog_first_byte_offset", "1500");
        assertThat(selector.unusable(info, MASTER_IP, checkpoint(REPLID, 1000))).isEqualTo("backlog starts at 1500 > 1001");

        info = healthyReplica();
        info.put("repl_backlog_active", "0");
        assertThat(selector.unusable(info, MASTER_IP, checkpoint(REPLID, 1000))).startsWith("backlog starts");
    }

    @Test
    void checkpointUnderThePreviousHistoryIsServedUpToTheFailoverPoint() {
        Map<String, String> info = healthyReplica();
        info.put("master_replid", OTHER_REPLID);
        info.put("master_replid2", REPLID);
        info.put("second_repl_offset", "1500");
        assertThat(selector.unusable(info, MASTER_IP, checkpoint(REPLID, 1000))).isNull();
        assertThat(selector.unusable(info, MASTER_IP, checkpoint(REPLID, 1500))).isEqualTo("replid mismatch");
    }

    @Test
    void unknownReplId() {
        assertThat(selector.unusable(healthyReplica(), MASTER_IP, checkpoint(OTHER_REPLID, 1000)))
            .isEqualTo("replid mismatch");
    }

    @Test
    void onlyHistoryProblemsCountAsCannotServe() {
        assertThat(selector.cannotServeCheckpoint("behind (1<2)")).isTrue();
        assertThat(selector.cannotServeCheckpoint("backlog starts at 5 > 2")).isTrue();
        assertThat(selector.cannotServeCheckpoint("replid mismatch")).isTrue();
        assertThat(selector.cannotServeCheckpoint("unreachable")).isFalse();
        assertThat(selector.cannotServeCheckpoint("link down")).isFalse();
        assertThat(selector.cannotServeCheckpoint("role=master")).isFalse();
    }

    private static Map<String, String> masterInfo(String replId, String replId2, String secondOffset) {
        Map<String, String> info = new HashMap<>();
        info.put("role", "master");
        info.put("master_replid", replId);
        info.put("master_replid2", replId2);
        info.put("second_repl_offset", secondOffset);
        return info;
    }

    @Test
    void reconcileKeepsTheCheckpointWhenNothingChanged() {
        assertThat(selector.reconcile(masterInfo(REPLID, "0".repeat(40), "-1"), checkpoint(REPLID, 1000)))
            .isEqualTo(Reconciliation.KEEP);
        assertThat(selector.reconcile(masterInfo(OTHER_REPLID, REPLID, "1500"), new Checkpoint()))
            .isEqualTo(Reconciliation.KEEP);
        Map<String, String> notMaster = masterInfo(OTHER_REPLID, REPLID, "500");
        notMaster.put("role", "slave");
        assertThat(selector.reconcile(notMaster, checkpoint(REPLID, 1000))).isEqualTo(Reconciliation.KEEP);
    }

    @Test
    void reconcileReanchorsWhenTheCheckpointIsBeforeTheFailoverPoint() {
        assertThat(selector.reconcile(masterInfo(OTHER_REPLID, REPLID, "1500"), checkpoint(REPLID, 1000)))
            .isEqualTo(Reconciliation.RE_ANCHOR);
    }

    @Test
    void reconcileDetectsADivergedHistory() {
        // the checkpoint is beyond what the new master inherited
        assertThat(selector.reconcile(masterInfo(OTHER_REPLID, REPLID, "900"), checkpoint(REPLID, 1000)))
            .isEqualTo(Reconciliation.DIVERGED);
        // the new master never knew the checkpoint's history
        assertThat(selector.reconcile(masterInfo(OTHER_REPLID, "c".repeat(40), "900"), checkpoint(REPLID, 1000)))
            .isEqualTo(Reconciliation.DIVERGED);
    }
}
