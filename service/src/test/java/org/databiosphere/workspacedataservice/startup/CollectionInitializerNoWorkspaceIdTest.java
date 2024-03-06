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
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@TestPropertySource(properties = {"twds.instance.workspace-id="})
@DirtiesContext
@SpringBootTest
class CollectionInitializerNoWorkspaceIdTest extends TestBase {
  // Don't run the CollectionInitializer on startup, so this test can start with a clean slate.
  // By making an (empty) mock bean to replace CollectionInitializer, we ensure it is a noop.
  @MockBean CollectionInitializer collectionInitializer;

  // Don't run StartupConfig to bypass the intentional crash on missing WORKSPACE_ID
  @MockBean StartupConfig startupConfig;
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
