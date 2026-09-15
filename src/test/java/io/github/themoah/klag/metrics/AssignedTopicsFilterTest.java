package io.github.themoah.klag.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.themoah.klag.model.ConsumerGroupLag;
import io.github.themoah.klag.model.ConsumerGroupLag.PartitionLag;
import io.github.themoah.klag.model.ConsumerGroupOffsets.TopicPartitionKey;
import io.github.themoah.klag.model.ConsumerGroupState;
import io.github.themoah.klag.model.ConsumerGroupState.State;
import io.github.themoah.klag.model.MemberAssignment;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Unit tests for {@link AssignedTopicsFilter}: Stable groups drop unassigned topics;
 * Empty / rebalancing / flag-off leave lag intact (#92).
 */
class AssignedTopicsFilterTest {

  private static final MemberAssignment OWNER =
    new MemberAssignment("/192.168.1.1", "consumer-1", "client-1");

  private static PartitionLag lag(String topic, int partition, long committed, long end) {
    return PartitionLag.of(topic, partition, end, 0, 0, 0, committed);
  }

  private static ConsumerGroupLag group(String id, PartitionLag... partitions) {
    return ConsumerGroupLag.fromPartitions(id, List.of(partitions));
  }

  private static ConsumerGroupState state(
      String id, State s, Map<TopicPartitionKey, MemberAssignment> owners) {
    return new ConsumerGroupState(id, s, owners);
  }

  @Test
  void flagOff_passesThroughUnchanged() {
    ConsumerGroupLag lag = group("g1",
      lag("active", 0, 10, 20),
      lag("abandoned", 0, 5, 100));
    Map<String, ConsumerGroupState> states = Map.of("g1",
      state("g1", State.STABLE, Map.of(new TopicPartitionKey("active", 0), OWNER)));

    List<ConsumerGroupLag> result = AssignedTopicsFilter.filter(List.of(lag), states, false);

    assertEquals(1, result.size());
    assertEquals(2, result.get(0).partitions().size());
  }

  @Test
  void stable_dropsTopicsWithNoAssignedPartition() {
    ConsumerGroupLag lag = group("g1",
      lag("active", 0, 10, 20),
      lag("active", 1, 10, 20),
      lag("abandoned", 0, 5, 100),
      lag("abandoned", 1, 5, 100));
    Map<String, ConsumerGroupState> states = Map.of("g1",
      state("g1", State.STABLE, Map.of(
        new TopicPartitionKey("active", 0), OWNER,
        new TopicPartitionKey("active", 1), OWNER)));

    List<ConsumerGroupLag> result = AssignedTopicsFilter.filter(List.of(lag), states, true);

    assertEquals(1, result.size());
    Set<String> topics = result.get(0).partitions().stream()
      .map(PartitionLag::topic).collect(Collectors.toSet());
    assertEquals(Set.of("active"), topics);
    assertEquals(20, result.get(0).totalLag());
  }

  @Test
  void stable_keepsWholeTopicWhenAnyPartitionAssigned() {
    // Topic still subscribed: one partition assigned is enough to keep all its lag rows.
    ConsumerGroupLag lag = group("g1",
      lag("orders", 0, 10, 20),
      lag("orders", 1, 10, 30));
    Map<String, ConsumerGroupState> states = Map.of("g1",
      state("g1", State.STABLE, Map.of(new TopicPartitionKey("orders", 0), OWNER)));

    List<ConsumerGroupLag> result = AssignedTopicsFilter.filter(List.of(lag), states, true);

    assertEquals(1, result.size());
    assertEquals(2, result.get(0).partitions().size());
    assertEquals(30, result.get(0).totalLag());
  }

  @Test
  void empty_keepsUnassignedTopics() {
    ConsumerGroupLag lag = group("g1", lag("abandoned", 0, 5, 100));
    Map<String, ConsumerGroupState> states = Map.of("g1",
      state("g1", State.EMPTY, Map.of()));

    List<ConsumerGroupLag> result = AssignedTopicsFilter.filter(List.of(lag), states, true);

    assertEquals(1, result.size());
    assertEquals(1, result.get(0).partitions().size());
    assertEquals("abandoned", result.get(0).partitions().get(0).topic());
  }

  @ParameterizedTest
  @EnumSource(value = State.class, names = {
    "PREPARING_REBALANCE", "COMPLETING_REBALANCE", "ASSIGNING", "RECONCILING",
    "DEAD", "UNKNOWN"
  })
  void nonStable_keepsAllTopics(State nonStable) {
    ConsumerGroupLag lag = group("g1",
      lag("active", 0, 10, 20),
      lag("abandoned", 0, 5, 100));
    Map<String, ConsumerGroupState> states = Map.of("g1",
      state("g1", nonStable, Map.of(new TopicPartitionKey("active", 0), OWNER)));

    List<ConsumerGroupLag> result = AssignedTopicsFilter.filter(List.of(lag), states, true);

    assertEquals(2, result.get(0).partitions().size());
  }

  @Test
  void stable_allTopicsUnassigned_omitsGroup() {
    ConsumerGroupLag lag = group("g1", lag("abandoned", 0, 5, 100));
    Map<String, ConsumerGroupState> states = Map.of("g1",
      state("g1", State.STABLE, Map.of()));

    List<ConsumerGroupLag> result = AssignedTopicsFilter.filter(List.of(lag), states, true);

    assertTrue(result.isEmpty());
  }

  @Test
  void missingState_passesGroupThrough() {
    ConsumerGroupLag lag = group("g1", lag("abandoned", 0, 5, 100));

    List<ConsumerGroupLag> result = AssignedTopicsFilter.filter(List.of(lag), Map.of(), true);

    assertEquals(1, result.size());
  }
}
