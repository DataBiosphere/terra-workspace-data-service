package org.databiosphere.workspacedataservice.shared.model;

public record EntityResponse(EntityId entityId, EntityType entityType, EntityAttributes entityAttributes, EntityMetadata entityMetadata) {}
