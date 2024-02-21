package org.databiosphere.workspacedataservice.annotations;

import static org.springframework.context.annotation.FilterType.ANNOTATION;

import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.junit.jupiter.params.provider.Arguments;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;

/* This is a minimal configuration for testing just the beans annotated with ControlPlane & DataPlane. */
@TestConfiguration
@ComponentScan(
    basePackages = "org.databiosphere.workspacedataservice",
    includeFilters = {
      @Filter(type = ANNOTATION, classes = ControlPlane.class),
      @Filter(type = ANNOTATION, classes = DataPlane.class)
    })
@SpringBootTest
class DeploymentModeTestBase {

  @Autowired protected ApplicationContext context;

  protected static Stream<Arguments> dataPlaneConditionalBeans() {
    return Stream.of(Arguments.of("collectionInitializer"));
  }

  protected static Stream<Arguments> controlPlaneConditionalBeans() {
    return Stream.of(Arguments.of());
  }
}
