package org.databiosphere.workspacedataservice;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.locks.Lock;
import org.databiosphere.workspacedataservice.activitylog.ActivityLoggerConfig;
import org.databiosphere.workspacedataservice.dao.*;
import org.databiosphere.workspacedataservice.distributed.DistributedLock;
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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles({
  "mock-instance-dao",
  "mock-backup-dao",
  "mock-restore-dao",
  "mock-clone-dao",
  "local"
})
@TestPropertySource(properties = {"twds.instance.workspace-id="})
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
      MockSamClientFactoryConfig.class
    })
class InstanceInitializerNoWorkspaceIdTest {

  @Autowired InstanceInitializerBean instanceInitializerBean;

  @SpyBean InstanceDao instanceDao;

  @MockBean DistributedLock mockDistributedLock;
  Lock mockLock = mock(Lock.class);

  @BeforeEach
  void beforeEach() {
    // always return a valid Lock object and always return true for acquiring that lock
    when(mockDistributedLock.obtainLock(anyString())).thenReturn(mockLock);
    when(mockDistributedLock.tryLock(any(Lock.class))).thenReturn(true);
  }

  @Test
  void workspaceIDNotProvidedNoExceptionThrown() {
    assertDoesNotThrow(() -> instanceInitializerBean.initializeInstance());
    // verify that method to create instance was NOT called
    verify(instanceDao, times(0)).createSchema(any());
  }
}
