package org.databiosphere.workspacedataservice.annotations;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.web.bind.annotation.RestController;

@ActiveProfiles(
    value = {"control-plane", "data-plane"},
    inheritProfiles = false)
@DirtiesContext
@SpringBootTest
class AnnotatedApisTest extends TestBase {

  @Autowired private ApplicationContext context;
  // Since we're running with both control-plane and data-plane profiles simultaneously, Spring
  // does not know what to do with the CollectionDao bean which is satisfied by two different
  // mutually exclusive beans.  By mocking it out here explicitly we avoid this ambiguity.
  @MockBean private CollectionDao collectionDao;

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
