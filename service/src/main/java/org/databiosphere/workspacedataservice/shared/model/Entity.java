package org.databiosphere.workspacedataservice.shared.model;


import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Entity {

    private EntityId name;

    private EntityType entityType;

    private EntityAttributes attributes;


    public Entity(EntityId name, EntityType entityType, EntityAttributes attributes) {
        this.name = name;
        this.entityType = entityType;
        this.attributes = attributes;
    }

    public Entity() {
    }

    public Entity(EntityId entityName) {
        this.name = entityName;
    }

    public Entity(EntityRequest request){
        this.name = request.entityId();
        this.entityType = request.entityType();
        this.attributes = request.entityAttributes();
    }

    public EntityId getName() {
        return name;
    }

    public void setName(EntityId name) {
        this.name = name;
    }

    public EntityAttributes getAttributes() {
        return attributes;
    }

    public void setAttributes(EntityAttributes attributes) {
        this.attributes = attributes;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    @JsonGetter("entityType")
    public String getEntityTypeName() {
        return entityType.getName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Entity entity)) return false;

        if (!getName().equals(entity.getName())) return false;
        return getEntityType().equals(entity.getEntityType());
    }

    @Override
    public int hashCode() {
        int result = getName().hashCode();
        result = 31 * result + getEntityType().hashCode();
        return result;
    }
}
