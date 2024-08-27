package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.CollectionId;

@Deprecated(forRemoval = true, since = "v0.18.0")
public interface CollectionDao {

  // 1 usage
  void alterSchema(CollectionId oldCollectionId, CollectionId newCollectionId);
}
