package org.databiosphere.workspacedataservice.service;

import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.databiosphere.workspacedataservice.dao.CollectionRepository;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.ConflictException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WdsCollection;
import org.databiosphere.workspacedataservice.shared.model.WdsCollectionCreateRequest;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.relational.core.conversion.DbActionExecutionException;
import org.springframework.stereotype.Component;

/** Service to power v1 collection APIs. */
@Component
public class CollectionServiceV1 {

  private final CollectionRepository collectionRepository;

  private static final Logger logger = LoggerFactory.getLogger(CollectionServiceV1.class);

  public CollectionServiceV1(CollectionRepository collectionRepository) {
    this.collectionRepository = collectionRepository;
  }

  /**
   * Insert a new collection
   *
   * @param workspaceId the workspace to contain this collection
   * @param collectionServerModel the collection definition
   * @return the created collection
   */
  public CollectionServerModel save(
      WorkspaceId workspaceId, CollectionServerModel collectionServerModel) {

    // TODO: check if user has write permission on this workspace
    // TODO: validate name against the CollectionServerModel.getName() pattern
    // TODO: create the schema in Postgres

    // if user did not specify an id, generate one
    CollectionId collectionId;
    if (collectionServerModel.getId() != null) {
      collectionId = CollectionId.of(collectionServerModel.getId());
    } else {
      collectionId = CollectionId.of(UUID.randomUUID());
    }
    // translate CollectionServerModel to WdsCollection
    WdsCollection wdsCollectionRequest =
        new WdsCollectionCreateRequest(
            workspaceId,
            collectionId,
            collectionServerModel.getName(),
            collectionServerModel.getDescription());

    // save
    WdsCollection actual = null;
    try {
      actual = collectionRepository.save(wdsCollectionRequest);
    } catch (DbActionExecutionException dbActionExecutionException) {
      handleDbException(dbActionExecutionException);
    }
    // translate back to CollectionServerModel
    CollectionServerModel response = new CollectionServerModel(actual.name(), actual.description());
    response.id(actual.collectionId().id());

    return response;
  }

  /**
   * Delete a collection.
   *
   * @param workspaceId the workspace containing the collection to be deleted
   * @param collectionId id of the collection to be deleted
   */
  public void delete(WorkspaceId workspaceId, CollectionId collectionId) {
    // TODO: ensure this collection belongs to this workspace
    // TODO: check if user has write permission on this workspace
    // TODO: delete the schema from Postgres

    collectionRepository.deleteById(collectionId.id());
  }

  /**
   * List all collections in a given workspace. Does not paginate.
   *
   * @param workspaceId the workspace in which to list collections.
   * @return all collections in the given workspace
   */
  public List<CollectionServerModel> list(WorkspaceId workspaceId) {
    // TODO: check if user has read permission on this workspace

    Iterable<WdsCollection> found = collectionRepository.findByWorkspace(workspaceId);
    // map the WdsCollection to CollectionServerModel
    Stream<WdsCollection> colls =
        StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(found.iterator(), Spliterator.ORDERED), false);

    return colls
        .map(
            wdsCollection -> {
              CollectionServerModel serverModel =
                  new CollectionServerModel(wdsCollection.name(), wdsCollection.description());
              serverModel.id(wdsCollection.collectionId().id());
              return serverModel;
            })
        .toList();
  }

  // exception handling
  private void handleDbException(DbActionExecutionException dbActionExecutionException) {
    Throwable cause = dbActionExecutionException.getCause();
    if (cause == null) {
      throw dbActionExecutionException;
    }
    if (cause instanceof DuplicateKeyException duplicateKeyException) {
      // kinda ugly: we need to determine if the DuplicateKeyException is due to an id conflict
      // or a name conflict.
      String msg = duplicateKeyException.getMessage();
      if (msg.contains("duplicate key value violates unique constraint")
          && msg.contains("instance_pkey")) {
        // TODO: this allows phishing for collection ids.
        throw new ConflictException("Collection with this id already exists");
      } else if (msg.contains("duplicate key value violates unique constraint")
          && msg.contains("instance_workspace_id_name_key")) {
        throw new ConflictException("Collection with this name already exists in this workspace");
      }
      throw new ConflictException("Collection name or id conflict");
    }
    throw dbActionExecutionException;
  }
}
