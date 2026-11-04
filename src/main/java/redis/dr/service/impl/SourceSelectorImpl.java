package redis.dr.service.impl;

import redis.dr.model.Checkpoint;
import redis.dr.service.SourceSelector;
import redis.dr.service.SourceTopology;

import java.util.Map;

public class SourceSelectorImpl implements SourceSelector {
    private final SourceTopology topology;

    public SourceSelectorImpl(SourceTopology topology) {
        this.topology = topology;
    }

    @Override
    public String unusable(Map<String, String> info, String masterIp, Checkpoint checkpoint) {
        if (!"slave".equals(info.get("role"))) return "role=" + info.get("role");
        if (!"up".equals(info.get("master_link_status"))) return "link down";
        try {
            if (!topology.resolveIp(info.getOrDefault("master_host", "")).equals(masterIp)) {
                return "not following current master (" + info.get("master_host") + ")";
            }
        } catch (Exception e) {
            return "master_host unresolved";
        }
        String replId = checkpoint.replId();
        long offset = checkpoint.offset();
        if (replId == null) return null; // full sync: any healthy replica of the current master will do
        long replOffset = Long.parseLong(info.get("master_repl_offset"));
        if (replOffset < offset) return "behind (" + replOffset + "<" + offset + ")";
        if (!"1".equals(info.get("repl_backlog_active"))
            || Long.parseLong(info.get("repl_backlog_first_byte_offset")) > offset + 1) {
            return "backlog starts at " + info.get("repl_backlog_first_byte_offset") + " > " + (offset + 1);
        }
        if (replId.equals(info.get("master_replid"))) return null;
        if (replId.equals(info.get("master_replid2")) && offset + 1 <= Long.parseLong(info.get("second_repl_offset"))) {
            return null;
        }
        return "replid mismatch";
    }

    @Override
    public Reconciliation reconcile(Map<String, String> masterInfo, Checkpoint checkpoint) {
        String replId = checkpoint.replId();
        long offset = checkpoint.offset();
        if (replId == null) return Reconciliation.KEEP;
        if (!"master".equals(masterInfo.get("role")) || masterInfo.get("master_replid").equals(replId)) {
            return Reconciliation.KEEP;
        }
        if (replId.equals(masterInfo.get("master_replid2"))
            && offset + 1 <= Long.parseLong(masterInfo.get("second_repl_offset"))) {
            return Reconciliation.RE_ANCHOR;
        }
        return Reconciliation.DIVERGED;
    }

    @Override
    public boolean cannotServeCheckpoint(String why) {
        return why.startsWith("behind") || why.startsWith("backlog starts") || why.equals("replid mismatch");
    }
}
