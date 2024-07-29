package org.databiosphere.workspacedataservice.controller;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.annotations.DeploymentMode.DataPlane;
import org.databiosphere.workspacedataservice.generated.CollectionApi;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

/**
 * Implementations for collection API routes. This implements CollectionApi, which is auto-generated
 * from our OpenAPI spec.
 */
@DataPlane
@RestController
public class CollectionController implements CollectionApi {

  private final CollectionService collectionService;

  public CollectionController(CollectionService collectionService) {
    this.collectionService = collectionService;
  }

  /**
   * POST /collections/v1/{workspaceId} : Create a collection in this workspace. If collection id is
   * specified in the request body, it must be a valid UUID. If omitted, the system will generate an
   * id.
   *
   * @param workspaceId Workspace id (required)
   * @param collectionServerModel The collection to create (required)
   * @return The collection just created. (status code 201)
   */
  @Override
  public ResponseEntity<CollectionServerModel> createCollectionV1(
      UUID workspaceId, CollectionServerModel collectionServerModel) {
    CollectionServerModel coll =
        collectionService.save(WorkspaceId.of(workspaceId), collectionServerModel);
    return new ResponseEntity<>(coll, HttpStatus.CREATED);
  }

  /**
   * DELETE /collections/v1/{workspaceId}/{collectionId} : Delete the specified collection.
   *
   * @param workspaceId Workspace id (required)
   * @param collectionId WdsCollection id (required)
   * @return WdsCollection has been deleted. (status code 204)
   */
  @Override
  public ResponseEntity<Void> deleteCollectionV1(UUID workspaceId, UUID collectionId) {
    collectionService.delete(WorkspaceId.of(workspaceId), CollectionId.of(collectionId));
    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  /**
   * GET /collections/v1/{workspaceId}/{collectionId} : Retrieve a single collection.
   *
   * @param workspaceId Workspace id (required)
   * @param collectionId WdsCollection id (required)
   * @return The collection object. (status code 200)
   */
  @Override
  public ResponseEntity<CollectionServerModel> getCollectionV1(
      UUID workspaceId, UUID collectionId) {
    CollectionServerModel coll =
        collectionService.get(WorkspaceId.of(workspaceId), CollectionId.of(collectionId));
    return new ResponseEntity<>(coll, HttpStatus.OK);
  }

  /**
   * GET /collections/v1/{workspaceId} : List all collections in this workspace.
   *
   * @param workspaceId Workspace id (required)
   * @return List of collections in this workspace. (status code 200)
   */
  @Override
  public ResponseEntity<List<CollectionServerModel>> listCollectionsV1(UUID workspaceId) {
    List<CollectionServerModel> collections = collectionService.list(WorkspaceId.of(workspaceId));
    return new ResponseEntity<>(collections, HttpStatus.OK);
  }

  /**
   * PUT /collections/v1/{workspaceId}/{collectionId} : Update the specified collection.
   * WdsCollection id is optional in the request body. If specified, it must match the collection id
   * specified in the url.
   *
   * @param workspaceId Workspace id (required)
   * @param collectionId WdsCollection id (required)
   * @param collectionServerModel The collection to update (required)
   * @return The collection just updated. (status code 200)
   */
  @Override
  public ResponseEntity<CollectionServerModel> updateCollectionV1(
      UUID workspaceId, UUID collectionId, CollectionServerModel collectionServerModel) {
    CollectionServerModel coll =
        collectionService.update(
            WorkspaceId.of(workspaceId), CollectionId.of(collectionId), collectionServerModel);
    return new ResponseEntity<>(coll, HttpStatus.OK);
  }
}