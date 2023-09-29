package org.databiosphere.workspacedataservice;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLoggerConfig;
import org.databiosphere.workspacedataservice.dao.*;
import org.databiosphere.workspacedataservice.distributed.MockSuccessfulDistributedLock;
import org.databiosphere.workspacedataservice.leonardo.LeonardoConfig;
import org.databiosphere.workspacedataservice.sam.MockSamClientFactoryConfig;
import org.databiosphere.workspacedataservice.sam.SamConfig;
import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceConfig;
import org.databiosphere.workspacedataservice.storage.AzureBlobStorage;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles({
  "mock-instance-dao",
  "mock-backup-dao",
  "mock-restore-dao",
  "mock-clone-dao",
  "local",
  "mock-sam"
})
@TestPropertySource(
    properties = {
      "twds.instance.workspace-id=90e1b179-9f83-4a6f-a8c2-db083df4cd03",
      "twds.instance.source-workspace-id=10000000-0000-0000-0000-000000000111",
    })
@DirtiesContext
@SpringBootTest(
    classes = {
      InstanceInitializerConfig.class,
      MockInstanceDaoConfig.class,
      MockRestoreDaoConfig.class,
      MockBackupDaoConfig.class,
      LeonardoConfig.class,
      WorkspaceDataServiceConfig.class,
      MockCloneDaoConfig.class,
      BackupRestoreService.class,
      AzureBlobStorage.class,
      WorkspaceManagerConfig.class,
      ActivityLoggerConfig.class,
      SamConfig.class,
      MockSamClientFactoryConfig.class,
      MockSuccessfulDistributedLock.class
    })
class InstanceInitializerBeanSuccesfulLockCloneTest {

  @Autowired InstanceInitializerBean instanceInitializerBean;
  @SpyBean InstanceDao instanceDao;

  @Value("${twds.instance.workspace-id}")
  String workspaceId;

  // randomly generated UUID
  final UUID instanceId = UUID.fromString("90e1b179-9f83-4a6f-a8c2-db083df4cd03");

  @BeforeEach
  void beforeEach() {
    // clean up any instances left in the db
    List<UUID> allInstances = instanceDao.listInstanceSchemas();
    allInstances.forEach(instanceId -> instanceDao.dropSchema(instanceId));
  }

  @Test
  void successfulLockClones() {
    // destination workspaceid schema does not exist
    assertFalse(instanceDao.instanceSchemaExists(instanceId));
    // acquire the lock successfully and clone
    assertDoesNotThrow(() -> instanceInitializerBean.initializeInstance());
    // destination workspaceid is the only one in the db
    List<UUID> expectedInstances = List.of(instanceId);
    List<UUID> actualInstances = instanceDao.listInstanceSchemas();
    assertEquals(expectedInstances, actualInstances);
  }
}
