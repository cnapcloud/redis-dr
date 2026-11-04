package redis.dr.model;

/**
 * Whether a paused partition can continue from its checkpoint without a full sync.
 *
 * @param resumable true when {@code resume} would continue from the checkpoint (or the partition is not paused)
 * @param reason    why it is or is not resumable
 */
public record ResumeCheck(boolean resumable, String reason) {
}
