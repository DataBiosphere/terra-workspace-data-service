package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonValue;

public class EntityType {

    public EntityType(String name) {
        this.name = name;
    }

    public EntityType() {
    }

    private String name;

    @JsonValue
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
