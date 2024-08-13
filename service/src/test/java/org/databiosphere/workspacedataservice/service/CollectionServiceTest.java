package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"mock-collection-dao"})
@DirtiesContext
@SpringBootTest
class CollectionServiceTest extends TestBase {

  @Autowired private CollectionService collectionService;
  @Autowired private CollectionDao collectionDao;
  @Autowired private TwdsProperties twdsProperties;

  private static final UUID COLLECTION = UUID.fromString("111e9999-e89b-12d3-a456-426614174000");

  @BeforeEach
  @AfterEach
  void dropCollectionSchemas() {
    // Delete all collections (v0.2)
    collectionDao
        .listCollectionSchemas()
        .forEach(collection -> collectionDao.dropSchema(collection));
    // Delete all collections in this workspace (v1)
    WorkspaceId workspaceId = twdsProperties.workspaceId();
    collectionService
        .list(workspaceId)
        .forEach(
            collection -> {
              collectionService.delete(workspaceId, CollectionId.of(collection.getId()));
            });
  }

  @Test
  void testExistsAndCreateDefault() {

    WorkspaceId workspaceId = twdsProperties.workspaceId();
    CollectionId collectionId = CollectionId.of(workspaceId.id());

    // exists should be false to start
    assertFalse(collectionService.exists(workspaceId, collectionId));

    // create default collection
    collectionService.createDefaultCollection(workspaceId);

    // exists should be true after we create the collection
    assertTrue(collectionService.exists(workspaceId, collectionId));

    // delete collection once more
    collectionService.delete(workspaceId, collectionId);

    // exists should be false again
    assertFalse(collectionService.exists(workspaceId, collectionId));
  }

  @Test
  void testCreateDefaultIsIdempotent() {
    WorkspaceId workspaceId = twdsProperties.workspaceId();
    CollectionId collectionId = CollectionId.of(workspaceId.id());

    // at the start of the test, we expect the default collection does not exist
    assertFalse(collectionService.exists(workspaceId, collectionId));
    assertThat(collectionService.list(workspaceId)).hasSize(0);

    // issue the call to create the default collection a few times; this call should be idempotent
    for (int i = 0; i < 5; i++) {
      collectionService.createDefaultCollection(workspaceId);
      assertTrue(collectionService.exists(workspaceId, collectionId));
      assertThat(collectionService.list(workspaceId)).hasSize(1);
    }
  }

  @Test
  void testFindAndCreateDefault() {

    WorkspaceId workspaceId = twdsProperties.workspaceId();
    CollectionId collectionId = CollectionId.of(workspaceId.id());

    // find should be empty to start
    assertThat(collectionService.find(workspaceId, collectionId)).isEmpty();

    // create default collection
    collectionService.createDefaultCollection(workspaceId);

    // find should be present after we create the collection
    var found = collectionService.find(workspaceId, collectionId);
    assertThat(found).isPresent();
    assertEquals(collectionId.id(), found.get().getId());

    // delete collection once more
    collectionService.delete(workspaceId, collectionId);

    // find should be empty again
    assertThat(collectionService.find(workspaceId, collectionId)).isEmpty();
  }

  // ========== following tests test the deprecated v0.2 CollectionService APIs
  @Test
  void testCreateAndValidateCollection() {
    collectionService.createCollection(COLLECTION, VERSION);
    collectionService.validateCollection(COLLECTION);

    UUID invalidCollection = UUID.fromString("000e4444-e22b-22d1-a333-426614174000");
    assertThrows(
        MissingObjectException.class,
        () -> collectionService.validateCollection(invalidCollection),
        "validateCollection should have thrown an error");
  }

  @Test
  void listCollections() {
    collectionService.createCollection(COLLECTION, VERSION);

    UUID secondCollectionId = UUID.fromString("999e1111-e89b-12d3-a456-426614174000");
    collectionService.createCollection(secondCollectionId, VERSION);

    List<UUID> collections = collectionService.listCollections(VERSION);

    assertThat(collections).hasSize(2).contains(COLLECTION).contains(secondCollectionId);
  }

  @Test
  void deleteCollection() {
    collectionService.createCollection(COLLECTION, VERSION);
    collectionService.validateCollection(COLLECTION);

    collectionService.deleteCollection(COLLECTION, VERSION);
    assertThrows(
        MissingObjectException.class,
        () -> collectionService.validateCollection(COLLECTION),
        "validateCollection should have thrown an error");
  }
}
