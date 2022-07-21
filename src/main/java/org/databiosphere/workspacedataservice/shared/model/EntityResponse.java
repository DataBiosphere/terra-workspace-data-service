package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record EntityResponse(
        @JsonProperty("id") EntityId entityId,
        @JsonProperty("type") EntityType entityType,
        @JsonProperty("attributes") EntityAttributes entityAttributes,
        @JsonProperty("metadata") EntityMetadata entityMetadata) {}
