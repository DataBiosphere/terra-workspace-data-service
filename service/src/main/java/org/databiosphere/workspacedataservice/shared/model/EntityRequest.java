package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"id", "type", "attributes"})
public record EntityRequest(
        @JsonProperty("id") EntityId entityId,
        @JsonProperty("type") EntityType entityType,
        @JsonProperty("attributes") EntityAttributes entityAttributes) {}
