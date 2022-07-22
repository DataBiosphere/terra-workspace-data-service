package org.databiosphere.workspacedataservice.shared.model;

public record EntityRequest(EntityId entityId, EntityType entityType, EntityAttributes entityAttributes) {}
