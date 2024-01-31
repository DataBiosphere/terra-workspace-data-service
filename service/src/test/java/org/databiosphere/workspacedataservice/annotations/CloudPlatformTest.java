package org.databiosphere.workspacedataservice.annotations;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.springframework.context.annotation.FilterType.ANNOTATION;

import org.databiosphere.workspacedataservice.annotations.CloudPlatform.Azure;
import org.databiosphere.workspacedataservice.annotations.CloudPlatform.GCP;
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
class CloudPlatformTest {
  @Test
  void instanceInitializerEnabledForAzure() {
    var context = loadApplicationContext("env.wds.cloudPlatform=azure");
    assertThat(context.containsBean("instanceInitializer")).isTrue();
  }

  @Test
  void instanceInitializerDisabledForGcp() {
    var context = loadApplicationContext("env.wds.cloudPlatform=gcp");
    assertThat(context.containsBean("instanceInitializer")).isFalse();
  }

  /* This is a minimal configuration for testing just the beans annotated with GCP & Azure. */
  @TestConfiguration
  @ComponentScan(
      basePackages = "org.databiosphere.workspacedataservice",
      includeFilters = {
        @Filter(type = ANNOTATION, classes = GCP.class),
        @Filter(type = ANNOTATION, classes = Azure.class)
      })
  static class MinimalTestConfiguration {}

  private ConfigurableApplicationContext loadApplicationContext(String... properties) {
    ConfigurableEnvironment environment = new StandardEnvironment();

    TestPropertyValues.of(properties).applyTo(environment);
    return new SpringApplicationBuilder(MinimalTestConfiguration.class)
        .environment(environment)
        .web(WebApplicationType.NONE)
        .run();
  }
}
