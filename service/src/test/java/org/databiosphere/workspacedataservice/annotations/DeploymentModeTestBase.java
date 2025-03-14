package org.databiosphere.workspacedataservice.annotations;

import static org.springframework.context.annotation.FilterType.ANNOTATION;

import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.test.annotation.DirtiesContext;

/* This is a minimal configuration for testing just the beans annotated with ControlPlane & DataPlane. */
@TestConfiguration
@ComponentScan(
    basePackages = "org.databiosphere.workspacedataservice",
    includeFilters = {
      @Filter(type = ANNOTATION, classes = ControlPlane.class),
      @Filter(type = ANNOTATION, classes = DataPlane.class)
    })
@DirtiesContext
@SpringBootTest
class DeploymentModeTestBase extends DataPlaneTestBase {

  @Autowired protected ApplicationContext context;

  protected static Stream<String> dataPlaneConditionalBeans() {
    return Stream.of("capabilitiesController", "collectionController", "recordController");
  }

  protected static Stream<String> controlPlaneConditionalBeans() {
    return Stream.of("rawlsJsonQuartzJob");
  }
}
