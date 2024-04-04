package org.databiosphere.workspacedataservice.common;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;

/**
 * Provides a manipulable extension of {@link InstantSource} for use in tests that require control
 * over time.
 */
public class MockInstantSource implements InstantSource {
  private Instant instant;

  public MockInstantSource(Instant defaultInstant) {
    this.instant = defaultInstant;
  }

  public MockInstantSource() {
    this(Instant.now());
  }

  @Override
  public Instant instant() {
    return instant;
  }

  public void setInstant(Instant instant) {
    this.instant = instant;
  }

  public void add(Duration time) {
    instant = instant.plus(time);
  }

  public void subtract(Duration time) {
    instant = instant.minus(time);
  }
}
