package org.databiosphere.workspacedataservice.common;

import static org.springframework.test.context.junit.jupiter.SpringExtension.getApplicationContext;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.observation.tck.TestObservationRegistry;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/** Extension to reset all Observation and Meter registries after each test. */
public class TestObservationRegistrySetupExtension implements AfterEachCallback {

  /** Reset all Observation and Meter registries after each test. */
  @Override
  public void afterEach(ExtensionContext context) {
    getApplicationContext(context).getBean(TestObservationRegistry.class).clear();
    getApplicationContext(context).getBean(MeterRegistry.class).clear();
    Metrics.globalRegistry.clear();
  }
}
