package org.databiosphere.workspacedataservice.startup;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.*;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.AfterEach;
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
  "local-cors",
  "mock-sam"
})
@TestPropertySource(
    properties = {"twds.instance.workspace-id=90e1b179-9f83-4a6f-a8c2-db083df4cd03"})
@DirtiesContext
@SpringBootTest
class CollectionInitializerBeanTest extends TestBase {
  // Don't run the CollectionInitializer on startup, so this test can start with a clean slate.
  // By making an (empty) mock bean to replace CollectionInitializer, we ensure it is a noop.
  @MockBean CollectionInitializer collectionInitializer;
  @Autowired CollectionInitializerBean collectionInitializerBean;
  @MockBean JdbcLockRegistry registry;
  @SpyBean CollectionDao collectionDao;
  @SpyBean CloneDao cloneDao;

  @Autowired @SingleTenant WorkspaceId workspaceId;

  // sourceWorkspaceId when we need one
  final String sourceWorkspaceId = UUID.randomUUID().toString();

  // randomly generated UUID
  final UUID collectionId = UUID.fromString("90e1b179-9f83-4a6f-a8c2-db083df4cd03");

  Lock mockLock = mock(Lock.class);

  @BeforeEach
  void setUp() throws InterruptedException {
    when(mockLock.tryLock(anyLong(), any())).thenReturn(true);
    when(registry.obtain(anyString())).thenReturn(mockLock);
  }

  @AfterEach
  void tearDown() {
    // clean up any collections left in the db
    List<UUID> allCollections = collectionDao.listCollectionSchemas();
    allCollections.forEach(collectionId -> collectionDao.dropSchema(collectionId));
  }

  @Test
  void testHappyPath() {
    // collection does not exist
    assertFalse(collectionDao.collectionSchemaExists(collectionId));
    assertDoesNotThrow(() -> collectionInitializerBean.initializeCollection());
    assert (collectionDao.collectionSchemaExists(collectionId));
  }

  @Test
  void testSchemaAlreadyExists() {
    // collection does not exist
    assertFalse(collectionDao.collectionSchemaExists(collectionId));
    // create the collection outside the initializer
    collectionDao.createSchema(collectionId);
    assertTrue(collectionDao.collectionSchemaExists(collectionId));
    // now run the initializer
    collectionInitializerBean.initializeCollection();
    // verify that method to create collection was NOT called again. We expect one call from the
    // setup
    // above.
    verify(collectionDao, times(1)).createSchema(any());
    assertTrue(collectionDao.collectionSchemaExists(collectionId));
  }

  @Test
  // Cloning where we can get a lock and complete successfully.
  void cloneSuccessfully() {
    // collection does not exist
    assertFalse(collectionDao.collectionSchemaExists(collectionId));
    // enter clone mode
    collectionInitializerBean.initCloneMode(sourceWorkspaceId);
    // confirm we have moved forward with cloning
    assertTrue(cloneDao.cloneExistsForWorkspace(UUID.fromString(sourceWorkspaceId)));
  }

  @Test
  // Cloning where we can't get a lock
  void cloneWithLockFail() throws InterruptedException {
    when(mockLock.tryLock(anyLong(), any())).thenReturn(false);
    // collection does not exist
    assertFalse(collectionDao.collectionSchemaExists(collectionId));
    // enter clone mode
    boolean cleanExit = collectionInitializerBean.initCloneMode(sourceWorkspaceId);
    // initCloneMode() should have returned true since we did not enter a situation
    // where we'd have to create the default schema.
    assertTrue(cleanExit);
    // confirm we did not enter clone mode
    assertFalse(cloneDao.cloneExistsForWorkspace(UUID.fromString(sourceWorkspaceId)));
  }

  @Test
  // Cloning where we can get lock, but entry already exists in clone table and default schema
  // exists.
  void cloneWithCloneTableAndCollectionExist() {
    // start with collection and clone entry
    collectionDao.createSchema(collectionId);
    cloneDao.createCloneEntry(UUID.randomUUID(), UUID.fromString(sourceWorkspaceId));
    // enter clone mode
    boolean cleanExit = collectionInitializerBean.initCloneMode(sourceWorkspaceId);
    // initCloneMode() should have returned true since we did not enter a situation
    // where we'd have to create the default schema.
    assertTrue(cleanExit);
  }

  @Test
  // Cloning where we can get lock, but entry already exists in clone table and default schema does
  // not exist.
  void cloneWithCloneTableAndNoCollection() {
    // start with clone entry
    cloneDao.createCloneEntry(UUID.randomUUID(), UUID.fromString(sourceWorkspaceId));
    // collection does not exist
    assertFalse(collectionDao.collectionSchemaExists(collectionId));
    // enter clone mode
    boolean cleanExit = collectionInitializerBean.initCloneMode(sourceWorkspaceId);
    // initCloneMode() should have returned false since we encountered a situation
    // where we'd have to create the default schema.
    assertFalse(cleanExit);
  }

  @Test
  void sourceWorkspaceIDNotProvided() {
    boolean cloneMode = collectionInitializerBean.isInCloneMode(null);
    assertFalse(cloneMode);
  }

  @Test
  void blankSourceWorkspaceID() {
    boolean cloneMode = collectionInitializerBean.isInCloneMode("");
    assertFalse(cloneMode);

    cloneMode = collectionInitializerBean.isInCloneMode(" ");
    assertFalse(cloneMode);
  }

  @Test
  void sourceWorkspaceIDCorrect() {
    boolean cloneMode = collectionInitializerBean.isInCloneMode(UUID.randomUUID().toString());
    assert (cloneMode);
  }

  @Test
  void sourceWorkspaceIDInvalid() {
    boolean cloneMode = collectionInitializerBean.isInCloneMode("invalidUUID");
    assertFalse(cloneMode);
  }

  @Test
  void sourceAndCurrentWorkspaceIdsMatch() {
    boolean cloneMode = collectionInitializerBean.isInCloneMode(workspaceId.toString());
    assertFalse(cloneMode);
  }
}
