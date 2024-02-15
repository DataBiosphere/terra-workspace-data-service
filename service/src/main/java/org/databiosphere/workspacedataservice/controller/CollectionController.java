package org.databiosphere.workspacedataservice.controller;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.retry.RetryableApi;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CollectionController {

  private final CollectionService collectionService;

  public CollectionController(CollectionService collectionService) {
    this.collectionService = collectionService;
  }

  @GetMapping("/collections/{version}")
  @RetryableApi
  public ResponseEntity<List<UUID>> listCollections(@PathVariable("version") String version) {
    List<UUID> schemaList = collectionService.listCollections(version);
    return new ResponseEntity<>(schemaList, HttpStatus.OK);
  }

  @PostMapping("/collections/{version}/{collectionId}")
  public ResponseEntity<String> createCollection(
      @PathVariable("collectionId") UUID collectionId, @PathVariable("version") String version) {
    collectionService.createCollection(collectionId, version);
    return new ResponseEntity<>(HttpStatus.CREATED);
  }

  @DeleteMapping("/collections/{version}/{collectionId}")
  public ResponseEntity<String> deleteCollection(
      @PathVariable("collectionId") UUID collectionId, @PathVariable("version") String version) {
    collectionService.deleteCollection(collectionId, version);
    return new ResponseEntity<>(HttpStatus.OK);
  }
}
