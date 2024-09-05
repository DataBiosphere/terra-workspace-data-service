package org.databiosphere.workspacedataservice.shared.model;

import org.databiosphere.workspacedataservice.generated.CollectionServerModel;

public record DefaultCollectionCreationResult(
    boolean created, CollectionServerModel collectionServerModel) {}
