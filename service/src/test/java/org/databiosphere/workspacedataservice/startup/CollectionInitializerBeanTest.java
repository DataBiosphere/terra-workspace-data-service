package org.databiosphere.workspacedataservice.startup;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import org.databiosphere.workspacedataservice.annotations.SingleTenant;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.*;
import org.databiosphere.workspacedataservice.leonardo.LeonardoDao;
import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.sourcewds.WorkspaceDataServiceDao;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;
import org.springframework.lang.Nullable;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({
  "mock-collection-dao",
  "mock-backup-dao",
  "mock-restore-dao",
  "mock-clone-dao",
  "local-cors",
  "mock-sam"
})
@DirtiesContext
@SpringBootTest
class CollectionInitializerBeanTest extends TestBase {
  // Don't run the CollectionInitializer on startup, so this test can start with a clean slate.
  // By making an (empty) mock bean to replace CollectionInitializer, we ensure it is a noop.
  @MockBean CollectionInitializer collectionInitializer;
  @Autowired LeonardoDao leoDao;
  @Autowired WorkspaceDataServiceDao wdsDao;
  @Autowired BackupRestoreService restoreService;
  @MockBean JdbcLockRegistry registry;
  @SpyBean CollectionDao collectionDao;
  @SpyBean CloneDao cloneDao;

  @Autowired @SingleTenant WorkspaceId workspaceId;

  // sourceWorkspaceId when we need one
  final WorkspaceId sourceWorkspaceId = WorkspaceId.of(randomUUID());

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
    assertFalse(collectionDao.collectionSchemaExists(collectionIdMatchingWorkspaceId()));
    assertDoesNotThrow(() -> getBean().initializeCollection());
    assert (collectionDao.collectionSchemaExists(collectionIdMatchingWorkspaceId()));
  }

  @Test
  void testSchemaAlreadyExists() {
    // collection does not exist
    assertFalse(collectionDao.collectionSchemaExists(collectionIdMatchingWorkspaceId()));
    // create the collection outside the initializer
    collectionDao.createSchema(collectionIdMatchingWorkspaceId());
    assertTrue(collectionDao.collectionSchemaExists(collectionIdMatchingWorkspaceId()));
    // now run the initializer
    getBean().initializeCollection();
    // verify that method to create collection was NOT called again. We expect one call from the
    // setup above.
    verify(collectionDao, times(1)).createSchema(any());
    assertTrue(collectionDao.collectionSchemaExists(collectionIdMatchingWorkspaceId()));
  }

  @Test
  // Cloning where we can get a lock and complete successfully.
  void cloneSuccessfully() {
    // collection does not exist
    assertFalse(collectionDao.collectionSchemaExists(collectionIdMatchingWorkspaceId()));
    // enter clone mode
    getBean().initCloneMode(sourceWorkspaceId);
    // confirm we have moved forward with cloning
    assertTrue(cloneDao.cloneExistsForWorkspace(sourceWorkspaceId));
  }

  @Test
  // Cloning where we can't get a lock
  void cloneWithLockFail() throws InterruptedException {
    when(mockLock.tryLock(anyLong(), any())).thenReturn(false);
    // collection does not exist
    assertFalse(collectionDao.collectionSchemaExists(collectionIdMatchingWorkspaceId()));
    // enter clone mode
    boolean cleanExit = getBean().initCloneMode(sourceWorkspaceId);
    // initCloneMode() should have returned true since we did not enter a situation
    // where we'd have to create the default schema.
    assertTrue(cleanExit);
    // confirm we did not enter clone mode
    assertFalse(cloneDao.cloneExistsForWorkspace(sourceWorkspaceId));
  }

  @Test
  // Cloning where we can get lock, but entry already exists in clone table and default schema
  // exists.
  void cloneWithCloneTableAndCollectionExist() {
    // start with collection and clone entry
    collectionDao.createSchema(collectionIdMatchingWorkspaceId());
    cloneDao.createCloneEntry(randomUUID(), sourceWorkspaceId);
    // enter clone mode
    boolean cleanExit = getBean().initCloneMode(sourceWorkspaceId);
    // initCloneMode() should have returned true since we did not enter a situation
    // where we'd have to create the default schema.
    assertTrue(cleanExit);
  }

  @Test
  // Cloning where we can get lock, but entry already exists in clone table and default schema does
  // not exist.
  void cloneWithCloneTableAndNoCollection() {
    // start with clone entry
    cloneDao.createCloneEntry(randomUUID(), sourceWorkspaceId);
    // collection does not exist
    assertFalse(collectionDao.collectionSchemaExists(collectionIdMatchingWorkspaceId()));
    // enter clone mode
    boolean cleanExit = getBean().initCloneMode(sourceWorkspaceId);
    // initCloneMode() should have returned false since we encountered a situation
    // where we'd have to create the default schema.
    assertFalse(cleanExit);
  }

  @Test
  void sourceWorkspaceIDNotProvided() {
    assertThat(getCloningSourceWorkspaceId(/* sourceWorkspaceIdString= */ null)).isEmpty();
  }

  @Test
  void blankSourceWorkspaceID() {
    assertThat(getCloningSourceWorkspaceId(/* sourceWorkspaceIdString= */ "")).isEmpty();
    assertThat(getCloningSourceWorkspaceId(/* sourceWorkspaceIdString= */ " ")).isEmpty();
  }

  @Test
  void sourceWorkspaceIDCorrect() {
    UUID randomUuid = randomUUID();
    assertThat(getCloningSourceWorkspaceId(/* sourceWorkspaceIdString= */ randomUuid.toString()))
        .hasValue(WorkspaceId.of(randomUuid));
  }

  @Test
  void sourceWorkspaceIDInvalid() {
    assertThat(getCloningSourceWorkspaceId("invalidUUID")).isEmpty();
  }

  @Test
  void sourceAndCurrentWorkspaceIdsMatch() {
    assertThat(getCloningSourceWorkspaceId(workspaceId.toString())).isEmpty();
  }

  private UUID collectionIdMatchingWorkspaceId() {
    return workspaceId.id();
  }

  private CollectionInitializerBean getBean() {
    return getBean(/* sourceWorkspaceIdString= */ null);
  }

  private CollectionInitializerBean getBean(@Nullable String sourceWorkspaceIdString) {
    return new CollectionInitializerBean(
        collectionDao,
        leoDao,
        wdsDao,
        cloneDao,
        restoreService,
        registry,
        workspaceId,
        sourceWorkspaceIdString,
        /* startupToken= */ null);
  }

  private Optional<WorkspaceId> getCloningSourceWorkspaceId(
      @Nullable String sourceWorkspaceIdString) {
    return getBean(sourceWorkspaceIdString).getCloningSourceWorkspaceId();
  }
}
