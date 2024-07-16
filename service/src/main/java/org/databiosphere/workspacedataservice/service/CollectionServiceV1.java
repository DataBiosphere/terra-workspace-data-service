package org.databiosphere.workspacedataservice.service;

import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.CollectionRepository;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WdsCollection;
import org.databiosphere.workspacedataservice.shared.model.WdsCollectionCreateRequest;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.springframework.stereotype.Component;

@Component
public class CollectionServiceV1 {

  private final CollectionRepository collectionRepository;

  public CollectionServiceV1(CollectionRepository collectionRepository) {
    this.collectionRepository = collectionRepository;
  }

  public CollectionServerModel save(
      WorkspaceId workspaceId, CollectionServerModel collectionServerModel) {
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
    WdsCollection actual = collectionRepository.save(wdsCollectionRequest);

    // translate back to CollectionServerModel
    CollectionServerModel response = new CollectionServerModel(actual.name(), actual.description());
    response.id(actual.collectionId().id());

    return response;
  }
}
