package org.databiosphere.workspacedataservice.startup;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.locks.Lock;
import org.databiosphere.workspacedataservice.activitylog.ActivityLoggerConfig;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.*;
import org.databiosphere.workspacedataservice.leonardo.LeonardoConfig;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
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
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles({
  "mock-collection-dao",
  "mock-backup-dao",
  "mock-restore-dao",
  "mock-clone-dao",
  "local-cors"
})
@TestPropertySource(properties = {"twds.instance.workspace-id="})
@DirtiesContext
@SpringBootTest(
    classes = {
      CollectionInitializerConfig.class,
      MockCollectionDaoConfig.class,
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
      RestClientRetry.class
    })
class CollectionInitializerNoWorkspaceIdTest extends TestBase {

  @Autowired CollectionInitializerBean collectionInitializerBean;
  @MockBean JdbcLockRegistry registry;
  @SpyBean CollectionDao collectionDao;

  Lock mockLock = mock(Lock.class);

  @BeforeEach
  void setUp() throws InterruptedException {
    when(mockLock.tryLock(anyLong(), any())).thenReturn(true);
    when(registry.obtain(anyString())).thenReturn(mockLock);
  }

  @Test
  void workspaceIDNotProvidedNoExceptionThrown() {
    assertDoesNotThrow(() -> collectionInitializerBean.initializeCollection());
    // verify that method to create collection was NOT called
    verify(collectionDao, times(0)).createSchema(any());
  }
}
