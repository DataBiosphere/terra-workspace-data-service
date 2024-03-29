package org.databiosphere.workspacedataservice.annotations;

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.databiosphere.workspacedataservice.common.TestObservationRegistrySetupExtension;
import org.databiosphere.workspacedataservice.observability.TestObservationRegistryConfig;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.context.annotation.Import;

/**
 * Tests can use this annotation to override the {@link ObservationRegistry} bean with a {@link
 * TestObservationRegistry} that can be autowired by the test to gain access to convenience
 * assertions for observations.
 *
 * <p>It also automatically adds an {@link @AfterEach} extension to clear all metrics and
 * observations after each test.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(TestObservationRegistryConfig.class)
@ExtendWith(TestObservationRegistrySetupExtension.class)
public @interface WithTestObservationRegistry {}
