package org.databiosphere.workspacedataservice.annotations;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.ControlPlane;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.dataimport.protecteddatasupport.ProtectedDataSupport;
import org.databiosphere.workspacedataservice.dataimport.protecteddatasupport.WsmProtectedDataSupport;
import org.databiosphere.workspacedataservice.dataimport.snapshotsupport.SnapshotSupportFactory;
import org.databiosphere.workspacedataservice.dataimport.snapshotsupport.WsmSnapshotSupportFactory;
import org.databiosphere.workspacedataservice.recordsink.RecordSinkFactory;
import org.databiosphere.workspacedataservice.recordsink.WdsRecordSinkFactory;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RecordService;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.bind.annotation.RestController;

@ActiveProfiles(
    value = {"control-plane", "data-plane"},
    inheritProfiles = false)
@DirtiesContext
@SpringBootTest
@TestPropertySource(
    properties = {
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false"
    })
class AnnotatedApisTest extends TestBase {

  // Since we're running with both control-plane and data-plane profiles simultaneously, Spring
  // does not know what to do with beans which are satisfied by two different mutually exclusive
  // implementations. In this @TestConfiguration, we explicitly specify @Primary beans for any
  // conflicts.
  @TestConfiguration
  static class SpecifyConflictingBeans {
    @Primary
    @Bean("overrideRecordSinkFactory")
    RecordSinkFactory overrideRecordSinkFactory(
        RecordService recordService, RecordDao recordDao, DataTypeInferer dataTypeInferer) {
      return new WdsRecordSinkFactory(recordService, recordDao, dataTypeInferer);
    }

    @Primary
    @Bean("overrideSnapshotSupportFactory")
    SnapshotSupportFactory overrideSnapshotSupportFactory(
        ActivityLogger activityLogger, WorkspaceManagerDao wsmDao) {
      return new WsmSnapshotSupportFactory(activityLogger, wsmDao);
    }

    @Primary
    @Bean("overrideProtectedDataSupport")
    ProtectedDataSupport overrideProtectedDataSupport(WorkspaceManagerDao wsmDao) {
      return new WsmProtectedDataSupport(wsmDao);
    }
  }

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
