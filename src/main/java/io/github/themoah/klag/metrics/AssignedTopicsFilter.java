package io.github.themoah.klag.metrics;

import io.github.themoah.klag.model.ConsumerGroupLag;
import io.github.themoah.klag.model.ConsumerGroupLag.PartitionLag;
import io.github.themoah.klag.model.ConsumerGroupOffsets.TopicPartitionKey;
import io.github.themoah.klag.model.ConsumerGroupState;
import io.github.themoah.klag.model.ConsumerGroupState.State;
import io.github.themoah.klag.model.MemberAssignment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drops lag for topics that a Stable consumer group has abandoned but whose committed
 * offsets have not yet expired ({@code offsets.retention.minutes}).
 *
 * <p>Uses member assignments already fetched via {@code describeConsumerGroups} — no
 * extra Kafka calls. Applied only when the group is {@link State#STABLE}; Empty and
 * rebalancing groups keep reporting so a total consumer outage is not hidden.
 */
public final class AssignedTopicsFilter {

  private static final Logger log = LoggerFactory.getLogger(AssignedTopicsFilter.class);

  private AssignedTopicsFilter() {}

  /**
   * Filters lag data to assigned topics when enabled.
   *
   * @param lagData per-group lag assembled from committed offsets
   * @param stateData group state + partition owners from the same cycle
   * @param enabled whether filtering is on
   * @return filtered lag list; groups with no remaining partitions are omitted
   */
  public static List<ConsumerGroupLag> filter(
      List<ConsumerGroupLag> lagData,
      Map<String, ConsumerGroupState> stateData,
      boolean enabled) {
    if (!enabled || lagData == null || lagData.isEmpty()) {
      return lagData;
    }

    List<ConsumerGroupLag> filtered = new ArrayList<>(lagData.size());
    for (ConsumerGroupLag group : lagData) {
      ConsumerGroupState state = stateData.get(group.consumerGroup());
      if (state == null || state.state() != State.STABLE) {
        filtered.add(group);
        continue;
      }

      Set<String> assignedTopics = assignedTopics(state.partitionOwners());
      List<PartitionLag> kept = group.partitions().stream()
        .filter(p -> assignedTopics.contains(p.topic()))
        .collect(Collectors.toList());

      if (kept.size() < group.partitions().size()) {
        log.debug("Group {}: dropped {} unassigned topic partition(s) (kept {})",
          group.consumerGroup(), group.partitions().size() - kept.size(), kept.size());
      }

      if (!kept.isEmpty()) {
        filtered.add(ConsumerGroupLag.fromPartitions(group.consumerGroup(), kept));
      }
    }
    return filtered;
  }

  private static Set<String> assignedTopics(Map<TopicPartitionKey, MemberAssignment> owners) {
    return owners.keySet().stream()
      .map(TopicPartitionKey::topic)
      .collect(Collectors.toSet());
  }
}
