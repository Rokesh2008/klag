package io.github.themoah.klag.metrics;

import io.github.themoah.klag.config.Env;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration for filtering lag metrics to topics with an active member assignment.
 *
 * <p>When enabled, Stable groups drop topics that still have committed offsets but no
 * member assigned to any of their partitions (abandoned after unsubscribe, until
 * {@code offsets.retention.minutes} expires). Empty and rebalancing groups are left
 * alone so outages are not hidden. Opt-in; default off preserves existing alert behaviour.
 *
 * @param enabled whether to filter lag reporting to assigned topics only
 */
public record AssignedTopicsConfig(
  boolean enabled
) {

  private static final Logger log = LoggerFactory.getLogger(AssignedTopicsConfig.class);
  private static final boolean DEFAULT_ENABLED = false;

  /**
   * Loads configuration from environment variables.
   *
   * <p>Supported environment variables:
   * <ul>
   *   <li>METRICS_ASSIGNED_TOPICS_ONLY - Drop unassigned topics for Stable groups (default: false)</li>
   * </ul>
   */
  public static AssignedTopicsConfig fromEnvironment() {
    boolean enabled = Env.getBool("METRICS_ASSIGNED_TOPICS_ONLY", DEFAULT_ENABLED);
    AssignedTopicsConfig config = new AssignedTopicsConfig(enabled);
    log.info("Assigned-topics filter: enabled={}", enabled);
    return config;
  }
}
