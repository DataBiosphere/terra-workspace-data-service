package org.databiosphere.workspacedataservice.shared.model;

/**
 * Extension of WdsCollection which overrides isNew() to return true. Use this subclass only for
 * cases in which we are inserting a new WdsCollection into the database. Spring Data checks the
 * isNew() method to see if this is an insert or update.
 */
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
