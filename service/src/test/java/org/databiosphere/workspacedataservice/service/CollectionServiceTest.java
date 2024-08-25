package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
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
  void testCreateDefaultIsIdempotent() {
    WorkspaceId workspaceId = twdsProperties.workspaceId();
    CollectionId collectionId = CollectionId.of(workspaceId.id());

    // at the start of the test, we expect the default collection does not exist
    assertThat(collectionService.find(workspaceId, collectionId)).isEmpty();
    assertThat(collectionService.list(workspaceId)).isEmpty();

    // issue the call to create the default collection a few times; this call should be idempotent
    for (int i = 0; i < 5; i++) {
      collectionService.createDefaultCollection(workspaceId);
      assertThat(collectionService.find(workspaceId, collectionId)).isPresent();
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
}
