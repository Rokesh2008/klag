package io.github.themoah.klag.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.themoah.klag.kafka.KafkaClientService;
import io.github.themoah.klag.model.ConsumerGroupOffsets;
import io.github.themoah.klag.model.ConsumerGroupOffsets.TopicPartitionKey;
import io.github.themoah.klag.model.ConsumerGroupState;
import io.github.themoah.klag.model.ConsumerGroupState.State;
import io.github.themoah.klag.model.MemberAssignment;
import io.github.themoah.klag.model.PartitionInfo;
import io.github.themoah.klag.model.PartitionOffsets;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * End-to-end: when {@code METRICS_ASSIGNED_TOPICS_ONLY=true}, Stable groups stop emitting
 * lag for topics with commits but no member assignment (#92).
 */
@ExtendWith(VertxExtension.class)
class MetricsCollectorAssignedTopicsTest {

  private static final String FLAG = "METRICS_ASSIGNED_TOPICS_ONLY";
  private static final MemberAssignment OWNER =
    new MemberAssignment("/10.0.0.1", "c-1", "client-1");

  @AfterEach
  void clearFlag() {
    System.clearProperty(FLAG);
  }

  /**
   * Group has commits on {@code active} (assigned) and {@code abandoned} (not assigned).
   * State is injectable so Empty vs Stable can be exercised.
   */
  private static class FakeKafka implements KafkaClientService {
    private final State groupState;
    private final Map<TopicPartitionKey, MemberAssignment> owners;

    FakeKafka(State groupState, Map<TopicPartitionKey, MemberAssignment> owners) {
      this.groupState = groupState;
      this.owners = owners;
    }

    @Override public Future<Set<String>> listTopics() {
      return Future.succeededFuture(Set.of("active", "abandoned"));
    }
    @Override public Future<List<PartitionInfo>> listPartitions(String topic) {
      return Future.succeededFuture(List.of());
    }
    @Override public Future<List<PartitionOffsets>> getLogEndOffsets(String topic) {
      return Future.succeededFuture(List.of(
        new PartitionOffsets(topic, 0, 100, 0, 0, 100, 0, 3, 3)));
    }
    @Override public Future<ConsumerGroupOffsets> getConsumerGroupOffsets(String groupId) {
      return Future.succeededFuture(new ConsumerGroupOffsets(groupId, Map.of(
        new TopicPartitionKey("active", 0), 90L,
        new TopicPartitionKey("abandoned", 0), 10L)));
    }
    @Override public Future<String> describeCluster() {
      return Future.succeededFuture("cluster");
    }
    @Override public Future<Set<String>> listConsumerGroups() {
      return Future.succeededFuture(Set.of("payments"));
    }
    @Override public Future<Map<String, ConsumerGroupState>> describeConsumerGroups(
        Set<String> groupIds) {
      return Future.succeededFuture(Map.of(
        "payments", new ConsumerGroupState("payments", groupState, owners)));
    }
    @Override public Future<Map<String, Long>> getTopicRetentionMs(Set<String> topics) {
      return Future.succeededFuture(Map.of());
    }
    @Override public Future<Void> close() {
      return Future.succeededFuture();
    }
  }

  @Test
  void disabledByDefault_reportsAbandonedTopic(Vertx vertx, VertxTestContext ctx) {
    FakeKafka kafka = new FakeKafka(State.STABLE,
      Map.of(new TopicPartitionKey("active", 0), OWNER));
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricsCollector collector = new MetricsCollector(vertx, kafka,
      new MicrometerReporter(registry), 60_000, "*");

    collector.collectOnce().onComplete(ctx.succeeding(v -> ctx.verify(() -> {
      assertNotNull(registry.find("klag.consumer.lag.sum")
        .tag("consumer_group", "payments").tag("topic", "abandoned").gauge());
      assertNotNull(registry.find("klag.consumer.lag.sum")
        .tag("consumer_group", "payments").tag("topic", "active").gauge());
      ctx.completeNow();
    })));
  }

  @Test
  void enabled_stable_dropsAbandonedTopic(Vertx vertx, VertxTestContext ctx) {
    System.setProperty(FLAG, "true");
    FakeKafka kafka = new FakeKafka(State.STABLE,
      Map.of(new TopicPartitionKey("active", 0), OWNER));
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricsCollector collector = new MetricsCollector(vertx, kafka,
      new MicrometerReporter(registry), 60_000, "*");

    collector.collectOnce().onComplete(ctx.succeeding(v -> ctx.verify(() -> {
      Gauge active = registry.find("klag.consumer.lag.sum")
        .tag("consumer_group", "payments").tag("topic", "active").gauge();
      assertNotNull(active);
      assertEquals(10.0, active.value());
      assertNull(registry.find("klag.consumer.lag.sum")
        .tag("consumer_group", "payments").tag("topic", "abandoned").gauge(),
        "Stable + assigned-only must drop topics with no member assignment");
      ctx.completeNow();
    })));
  }

  @Test
  void enabled_empty_keepsAbandonedTopic(Vertx vertx, VertxTestContext ctx) {
    System.setProperty(FLAG, "true");
    FakeKafka kafka = new FakeKafka(State.EMPTY, Map.of());
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    MetricsCollector collector = new MetricsCollector(vertx, kafka,
      new MicrometerReporter(registry), 60_000, "*");

    collector.collectOnce().onComplete(ctx.succeeding(v -> ctx.verify(() -> {
      assertNotNull(registry.find("klag.consumer.lag.sum")
        .tag("consumer_group", "payments").tag("topic", "abandoned").gauge(),
        "Empty groups must keep reporting so outages are not hidden");
      ctx.completeNow();
    })));
  }
}
