package org.databiosphere.workspacedataservice.controller;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.CollectionApi;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.springframework.http.ResponseEntity;

public class CollectionController implements CollectionApi {
  /**
   * POST /collections/v1/{workspaceId} : Create a collection in this workspace. If collection id is
   * specified in the request body, it must be a valid UUID. If omitted, the system will generate an
   * id.
   *
   * @param workspaceId Workspace id (required)
   * @param collectionServerModel The collection to create (required)
   * @return The collection just created. (status code 200)
   */
  @Override
  public ResponseEntity<CollectionServerModel> createCollectionV1(
      UUID workspaceId, CollectionServerModel collectionServerModel) {
    return CollectionApi.super.createCollectionV1(workspaceId, collectionServerModel);
  }

  /**
   * DELETE /collections/v1/{workspaceId}/{collectionId} : Delete the specified collection.
   *
   * @param workspaceId Workspace id (required)
   * @param collectionId Collection id (required)
   * @return Collection has been deleted. (status code 204)
   */
  @Override
  public ResponseEntity<Void> deleteCollectionV1(UUID workspaceId, UUID collectionId) {
    return CollectionApi.super.deleteCollectionV1(workspaceId, collectionId);
  }

  /**
   * GET /collections/v1/{workspaceId}/{collectionId} : Retrieve a single collection.
   *
   * @param workspaceId Workspace id (required)
   * @param collectionId Collection id (required)
   * @return The collection object. (status code 200)
   */
  @Override
  public ResponseEntity<CollectionServerModel> getCollectionV1(
      UUID workspaceId, UUID collectionId) {
    return CollectionApi.super.getCollectionV1(workspaceId, collectionId);
  }

  /**
   * GET /collections/v1/{workspaceId} : List all collections in this workspace.
   *
   * @param workspaceId Workspace id (required)
   * @return List of collections in this workspace. (status code 200)
   */
  @Override
  public ResponseEntity<List<CollectionServerModel>> listCollectionsV1(UUID workspaceId) {
    return CollectionApi.super.listCollectionsV1(workspaceId);
  }

  /**
   * PUT /collections/v1/{workspaceId}/{collectionId} : Update the specified collection. Collection
   * id is optional in the request body. If specified, it must match the collection id specified in
   * the url.
   *
   * @param workspaceId Workspace id (required)
   * @param collectionId Collection id (required)
   * @param collectionServerModel The collection to update (required)
   * @return The collection just updated. (status code 200)
   */
  @Override
  public ResponseEntity<CollectionServerModel> updateCollectionV1(
      UUID workspaceId, UUID collectionId, CollectionServerModel collectionServerModel) {
    return CollectionApi.super.updateCollectionV1(workspaceId, collectionId, collectionServerModel);
  }
}
