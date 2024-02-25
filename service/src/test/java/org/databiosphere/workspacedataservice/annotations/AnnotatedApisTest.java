package org.databiosphere.workspacedataservice.annotations;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.bind.annotation.RestController;

@ActiveProfiles(
    value = {"control-plane", "data-plane"},
    inheritProfiles = false)
@SpringBootTest
class AnnotatedApisTest extends TestBase {

  @Autowired private ApplicationContext context;

  @Test
  void controllersMustHaveDeploymentAnnotation() {
    String[] beanNames = context.getBeanNamesForAnnotation(RestController.class);
    // every @RestController should also be annotated with @DataPlane and/or @ControlPlane
    for (String bean : beanNames) {
      Optional<DataPlane> dataPlaneAnnotation =
          Optional.ofNullable(context.findAnnotationOnBean(bean, DataPlane.class));
      Optional<ControlPlane> controlPlaneAnnotation =
          Optional.ofNullable(context.findAnnotationOnBean(bean, ControlPlane.class));

      assertTrue(
          dataPlaneAnnotation.isPresent() || controlPlaneAnnotation.isPresent(),
          "No platform annotation on class " + bean);
    }
  }
}
