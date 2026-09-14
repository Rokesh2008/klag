package io.github.themoah.klag.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.themoah.klag.kafka.KafkaClientService;
import io.github.themoah.klag.model.ConsumerGroupOffsets;
import io.github.themoah.klag.model.ConsumerGroupState;
import io.github.themoah.klag.model.PartitionInfo;
import io.github.themoah.klag.model.PartitionOffsets;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class KafkaHealthMonitorTest {

  private static class UpKafka implements KafkaClientService {
    @Override public Future<Set<String>> listTopics() { return Future.succeededFuture(Set.of()); }
    @Override public Future<List<PartitionInfo>> listPartitions(String topic) { return Future.succeededFuture(List.of()); }
    @Override public Future<List<PartitionOffsets>> getLogEndOffsets(String topic) { return Future.succeededFuture(List.of()); }
    @Override public Future<ConsumerGroupOffsets> getConsumerGroupOffsets(String groupId) {
      return Future.succeededFuture(new ConsumerGroupOffsets(groupId, Map.of()));
    }
    @Override public Future<String> describeCluster() { return Future.succeededFuture("cluster-123"); }
    @Override public Future<Set<String>> listConsumerGroups() { return Future.succeededFuture(Set.of()); }
    @Override public Future<Map<String, ConsumerGroupState>> describeConsumerGroups(Set<String> groupIds) {
      return Future.succeededFuture(Map.of());
    }
    @Override public Future<Map<String, Long>> getTopicRetentionMs(Set<String> topics) { return Future.succeededFuture(Map.of()); }
    @Override public Future<Void> close() { return Future.succeededFuture(); }
  }

  private static class DownKafka implements KafkaClientService {
    @Override public Future<Set<String>> listTopics() { return Future.failedFuture("down"); }
    @Override public Future<List<PartitionInfo>> listPartitions(String topic) { return Future.failedFuture("down"); }
    @Override public Future<List<PartitionOffsets>> getLogEndOffsets(String topic) { return Future.failedFuture("down"); }
    @Override public Future<ConsumerGroupOffsets> getConsumerGroupOffsets(String groupId) { return Future.failedFuture("down"); }
    @Override public Future<String> describeCluster() { return Future.failedFuture(new RuntimeException("broker down")); }
    @Override public Future<Set<String>> listConsumerGroups() { return Future.failedFuture("down"); }
    @Override public Future<Map<String, ConsumerGroupState>> describeConsumerGroups(Set<String> groupIds) { return Future.failedFuture("down"); }
    @Override public Future<Map<String, Long>> getTopicRetentionMs(Set<String> topics) { return Future.failedFuture("down"); }
    @Override public Future<Void> close() { return Future.succeededFuture(); }
  }

  @Test
  void startSucceedsWhenKafkaIsDown(Vertx vertx, VertxTestContext ctx) {
    KafkaHealthMonitor monitor = new KafkaHealthMonitor(vertx, new DownKafka(), 60_000);
    assertEquals(HealthStatus.DOWN, monitor.getKafkaStatus());
    assertFalse(monitor.isKafkaConnected());

    monitor.start()
      .onComplete(ctx.succeeding(v -> ctx.verify(() -> {
        assertEquals(HealthStatus.DOWN, monitor.getKafkaStatus());
        assertFalse(monitor.isKafkaConnected());
        monitor.stop().onComplete(ctx.succeedingThenComplete());
      })));
  }

  @Test
  void startSucceedsWhenKafkaIsUp(Vertx vertx, VertxTestContext ctx) {
    KafkaHealthMonitor monitor = new KafkaHealthMonitor(vertx, new UpKafka(), 60_000);

    monitor.start()
      .onComplete(ctx.succeeding(v -> {
        vertx.setTimer(100, timerId -> ctx.verify(() -> {
          assertEquals(HealthStatus.UP, monitor.getKafkaStatus());
          assertTrue(monitor.isKafkaConnected());
          monitor.stop().onComplete(ctx.succeedingThenComplete());
        }));
      }));
  }
}
