package io.github.themoah.klag.metrics;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AssignedTopicsConfigTest {

  private static final String FLAG = "METRICS_ASSIGNED_TOPICS_ONLY";

  @AfterEach
  void clearFlag() {
    System.clearProperty(FLAG);
  }

  @Test
  void defaultsToDisabled() {
    assertFalse(AssignedTopicsConfig.fromEnvironment().enabled());
  }

  @Test
  void enabledViaSystemProperty() {
    System.setProperty(FLAG, "true");
    assertTrue(AssignedTopicsConfig.fromEnvironment().enabled());
  }
}
