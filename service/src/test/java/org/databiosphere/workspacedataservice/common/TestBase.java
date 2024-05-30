package org.databiosphere.workspacedataservice.common;

import org.databiosphere.workspacedataservice.config.ConfigurationException;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

/**
 * Optional base class for any test class annotated with {@link SpringBootTest} to automatically
 * enable the "data-plane" profile and a "workspace-id".
 *
 * <p>Subclasses can append additional active profiles by using the {@link ActiveProfiles}
 * annotation.
 *
 * <p>To explicitly override the "data-plane" profile, subclasses can use
 * {@code @ActiveProfiles(value={"some-profile"}, inheritProfiles=false)}.
 *
 * <p>To explicitly override the "workspace-id" that is configured, subclasses can use
 * {@code @TestPropertySource(properties={"twds.instance.workspace-id=<...>"})}.
 *
 * <p>Note: extending this class also extends the {@link ConfigurationExceptionDetector} class,
 * which will automatically provide a hint if it detects a test failure due to a {@link
 * ConfigurationException}.
 */
@ActiveProfiles({"data-plane"})
@TestPropertySource(
    properties = {
      // data-plane mode requires a workspace-id to be set
      // example uuid from https://en.wikipedia.org/wiki/Universally_unique_identifier
      "twds.instance.workspace-id=" + TestBase.HARDCODED_WORKSPACE_ID,
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false",
      // aggressive retry settings so unit tests don't run too long
      "rest.retry.maxAttempts=2",
      "rest.retry.backoff.delay=3",
      // Rawls url must be valid, else context initialization (Spring startup) will fail
      "rawlsUrl=https://localhost/"
    })
@ExtendWith(ConfigurationExceptionDetector.class)
public abstract class TestBase {
  public static final String HARDCODED_WORKSPACE_ID = "123e4567-e89b-12d3-a456-426614174000";
}
