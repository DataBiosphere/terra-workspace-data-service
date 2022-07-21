package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonValue;

public record EntityId(String entityIdentifier) {
    @JsonValue
    public String jsonRepresentation() {
        return this.entityIdentifier;
    }
}
