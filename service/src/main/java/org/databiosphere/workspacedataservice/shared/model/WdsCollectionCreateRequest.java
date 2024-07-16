package org.databiosphere.workspacedataservice.shared.model;

/** Extension of */
public class WdsCollectionCreateRequest extends WdsCollection {
  public WdsCollectionCreateRequest(
      WorkspaceId workspaceId, CollectionId collectionId, String name, String description) {
    super(workspaceId, collectionId, name, description);
  }

  @Override
  public boolean isNew() {
    return true;
  }
}
