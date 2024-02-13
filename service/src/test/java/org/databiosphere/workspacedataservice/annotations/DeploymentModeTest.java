package org.databiosphere.workspacedataservice.annotations;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.springframework.context.annotation.FilterType.ANNOTATION;

import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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

  private static Stream<Arguments> conditionalBeans() {
    return Stream.of(Arguments.of("collectionInitializer", "data-plane"));
  }

  @ParameterizedTest(name = "{0} is enabled for DeploymentMode {1} only")
  @MethodSource("conditionalBeans")
  void beansEnabledForDataPlaneOnly(String beanName, String deploymentMode) {
    assertThat(loadApplicationContext("env.wds.deploymentMode=" + deploymentMode).getBean(beanName))
        .isNotNull();

    assertThat(
            loadApplicationContext(
                    "env.wds.deploymentMode=" + differentDeploymentMode(deploymentMode))
                .containsBean(beanName))
        .isFalse();
  }

  private ConfigurableApplicationContext loadApplicationContext(String... properties) {
    ConfigurableEnvironment environment = new StandardEnvironment();

    TestPropertyValues.of(properties).applyTo(environment);
    return new SpringApplicationBuilder(MinimalTestConfiguration.class)
        .environment(environment)
        .web(WebApplicationType.NONE)
        .run();
  }

  private static String differentDeploymentMode(String deploymentMode) {
    return switch (deploymentMode) {
      case "control-plane" -> "data-plane";
      case "data-plane" -> "control-plane";
      default -> throw new IllegalArgumentException("Unknown deployment mode: " + deploymentMode);
    };
  }
}
