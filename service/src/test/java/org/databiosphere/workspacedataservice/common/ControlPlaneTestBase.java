package org.databiosphere.workspacedataservice.common;

import org.databiosphere.workspacedataservice.config.ConfigurationException;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

/**
 * Optional base class for any test class annotated with {@link SpringBootTest} to automatically
 * enable the "control-plane" profile.
 *
 * <p>Subclasses can append additional active profiles by using the {@link ActiveProfiles}
 * annotation.
 *
 * <p>To explicitly override the "control-plane" profile, subclasses can use
 * {@code @ActiveProfiles(value={"some-profile"}, inheritProfiles=false)}.
 *
 * <p>Note: extending this class also extends the {@link ConfigurationExceptionDetector} class,
 * which will automatically provide a hint if it detects a test failure due to a {@link
 * ConfigurationException}.
 */
@ActiveProfiles({"control-plane"})
@TestPropertySource(
    properties = {
      // enable all controllers
      "controlPlanePreview=on",
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false",
      // aggressive retry settings so unit tests don't run too long
      "rest.retry.maxAttempts=2",
      "rest.retry.backoff.delay=3",
      // Rawls url must be valid, else context initialization (Spring startup) will fail
      "rawlsUrl=https://localhost/",
      "drshuburl=https://localhost/"
    })
@ExtendWith(ConfigurationExceptionDetector.class)
public abstract class ControlPlaneTestBase {}
