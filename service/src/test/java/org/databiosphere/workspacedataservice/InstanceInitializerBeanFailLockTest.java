package org.databiosphere.workspacedataservice;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import org.databiosphere.workspacedataservice.activitylog.ActivityLoggerConfig;
import org.databiosphere.workspacedataservice.dao.*;
import org.databiosphere.workspacedataservice.distributed.DistributedLock;
import org.databiosphere.workspacedataservice.distributed.MockFailedDistributedLock;
import org.databiosphere.workspacedataservice.distributed.MockSuccessfulDistributedLock;
import org.databiosphere.workspacedataservice.leonardo.LeonardoConfig;
import org.databiosphere.workspacedataservice.leonardo.LeonardoDao;
import org.databiosphere.workspacedataservice.sam.MockSamClientFactoryConfig;
import org.databiosphere.workspacedataservice.sam.SamConfig;
import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceConfig;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceDao;
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
    properties = {"twds.instance.workspace-id=90e1b179-9f83-4a6f-a8c2-db083df4cd03"})
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
      MockFailedDistributedLock.class
    })
class InstanceInitializerBeanFailLockTest {

  @SpyBean InstanceDao instanceDao;

  @Value("${twds.instance.workspace-id}")
  String workspaceId;

  // randomly generated UUID
  private final UUID instanceId = UUID.randomUUID();
  @Autowired private LeonardoDao leoDao;
  @Autowired private WorkspaceDataServiceDao wdsDao;
  @Autowired private CloneDao cloneDao;
  @Autowired private BackupRestoreService backupRestoreService;

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
    assertDoesNotThrow(
        () ->
            createInstanceInitializerBean(new MockSuccessfulDistributedLock())
                .initializeInstance());
    // destination workspaceid is the only one in the db
    List<UUID> expectedInstances = List.of(instanceId);
    List<UUID> actualInstances = instanceDao.listInstanceSchemas();
    assertEquals(expectedInstances, actualInstances);
  }

  @Test
  void failedLockCreatesNoDefaultSchema() {
    // instance does not exist
    assertFalse(instanceDao.instanceSchemaExists(instanceId));
    assertDoesNotThrow(
        () -> createInstanceInitializerBean(new MockFailedDistributedLock()).initializeInstance());
    // instance still does not exist because lock failed to be acquired
    assertFalse(instanceDao.instanceSchemaExists(instanceId));
  }

  @Test
  void otherMockTestWithFancyScenariosBasedOnMockito() {

    DistributedLock mockOuterLock = mock(DistributedLock.class);
    Lock mockInnerLock = mock(Lock.class);
    // fail once, then succeed
    when(mockInnerLock.tryLock()).thenThrow(new InterruptedException("Failed!")).thenReturn(true);
    when(mockOuterLock.obtainLock(anyString())).thenReturn(mockInnerLock);

    // ... assertions
    assertDoesNotThrow(() -> createInstanceInitializerBean(mockOuterLock).initializeInstance());
  }

  private InstanceInitializerBean createInstanceInitializerBean(DistributedLock lock) {
    return new InstanceInitializerBean(
        instanceDao, leoDao, wdsDao, cloneDao, backupRestoreService, lock);
  }
}
