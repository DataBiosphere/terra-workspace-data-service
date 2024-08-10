package org.databiosphere.workspacedataservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
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

  private static final UUID COLLECTION = UUID.fromString("111e9999-e89b-12d3-a456-426614174000");

  @BeforeEach
  @AfterEach
  void dropCollectionSchemas() {
    // Delete all collections
    collectionDao
        .listCollectionSchemas()
        .forEach(collection -> collectionDao.dropSchema(collection));
  }

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
