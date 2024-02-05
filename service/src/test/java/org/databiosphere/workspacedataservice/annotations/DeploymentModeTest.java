package org.databiosphere.workspacedataservice.annotations;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.springframework.context.annotation.FilterType.ANNOTATION;

import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.StandardEnvironment;

@SpringBootTest
class DeploymentModeTest {
  /* This is a minimal configuration for testing just the beans annotated with ControlPlane & DataPlane. */
  @TestConfiguration
  @ComponentScan(
      basePackages = "org.databiosphere.workspacedataservice",
      includeFilters = {
        @Filter(type = ANNOTATION, classes = ControlPlane.class),
        @Filter(type = ANNOTATION, classes = DataPlane.class)
      })
  static class MinimalTestConfiguration {}

  @Test
  void instanceInitializerEnabledForAzure() {
    var context = loadApplicationContext("env.wds.deploymentMode=data-plane");
    assertThat(context.containsBean("instanceInitializerBean")).isTrue();
  }

  @Test
  void instanceInitializerDisabledForGcp() {
    var context = loadApplicationContext("env.wds.deploymentMode=control-plane");
    assertThat(context.containsBean("instanceInitializerBean")).isFalse();
  }

  private ConfigurableApplicationContext loadApplicationContext(String... properties) {
    ConfigurableEnvironment environment = new StandardEnvironment();

    TestPropertyValues.of(properties).applyTo(environment);
    return new SpringApplicationBuilder(MinimalTestConfiguration.class)
        .environment(environment)
        .web(WebApplicationType.NONE)
        .run();
  }
}
