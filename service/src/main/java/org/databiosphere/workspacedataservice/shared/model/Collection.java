package org.databiosphere.workspacedataservice.shared.model;

public record Collection(
    WorkspaceId workspaceId, CollectionId collectionId, String name, String description) {}
