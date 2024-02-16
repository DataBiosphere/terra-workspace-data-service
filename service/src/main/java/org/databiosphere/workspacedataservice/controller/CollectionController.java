package org.databiosphere.workspacedataservice.controller;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.CollectionsApi;
import org.databiosphere.workspacedataservice.retry.RetryableApi;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CollectionController implements CollectionsApi {

  private final CollectionService collectionService;
  // TODO is this the correct way to deal with the version
  private final String version = "v1";

  public CollectionController(CollectionService collectionService) {
    this.collectionService = collectionService;
  }

  @Override
  @RetryableApi
  public ResponseEntity<List<UUID>> listCollectionsV1() {
    List<UUID> schemaList = collectionService.listCollections(version);
    return new ResponseEntity<>(schemaList, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<Void> createCollectionV1(@PathVariable("collectionId") UUID collectionId) {
    collectionService.createCollection(collectionId, version);
    return new ResponseEntity<>(HttpStatus.CREATED);
  }

  @Override
  public ResponseEntity<Void> deleteCollectionV1(@PathVariable("collectionId") UUID collectionId) {
    collectionService.deleteCollection(collectionId, version);
    return new ResponseEntity<>(HttpStatus.OK);
  }
}
