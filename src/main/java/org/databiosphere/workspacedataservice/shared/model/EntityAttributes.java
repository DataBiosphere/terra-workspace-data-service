package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Map;

public record EntityAttributes(Map<String, Object> attributes) {
    @JsonValue
    public Map<String, Object> jsonRepresentation() {
        return this.attributes;
    }
}
