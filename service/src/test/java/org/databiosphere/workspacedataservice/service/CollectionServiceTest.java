package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLoggerConfig;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.MockCollectionDaoConfig;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.sam.MockSamClientFactoryConfig;
import org.databiosphere.workspacedataservice.sam.SamConfig;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"mock-sam", "mock-collection-dao"})
@DirtiesContext
@SpringBootTest(
    classes = {
      CollectionService.class,
      MockCollectionDaoConfig.class,
      SamConfig.class,
      MockSamClientFactoryConfig.class,
      ActivityLoggerConfig.class,
      RestClientRetry.class,
      TwdsProperties.class
    })
class CollectionServiceTest {

  @Autowired private CollectionService collectionService;
  @Autowired private CollectionDao collectionDao;

  private static final UUID COLLECTION = UUID.fromString("111e9999-e89b-12d3-a456-426614174000");

  @AfterEach
  void tearDown() {
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

    // clean up
    collectionService.deleteCollection(COLLECTION, VERSION);
  }

  @Test
  void listCollections() {
    collectionService.createCollection(COLLECTION, VERSION);

    UUID secondCollectionId = UUID.fromString("999e1111-e89b-12d3-a456-426614174000");
    collectionService.createCollection(secondCollectionId, VERSION);

    List<UUID> collections = collectionService.listCollections(VERSION);

    assertEquals(2, collections.size());
    assert (collections.contains(COLLECTION));
    assert (collections.contains(secondCollectionId));

    collectionService.deleteCollection(COLLECTION, VERSION);
    collectionService.deleteCollection(secondCollectionId, VERSION);
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
