package org.databiosphere.workspacedataservice.shared.model;


import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Entity {

    private EntityId name;

    private Boolean deleted;

    private EntityType entityType;

    private Map<String, Object> attributes;

    private Long entityTypeId;

    public Entity(EntityId name, EntityType entityType, Map<String, Object> attributes) {
        this.name = name;
        this.entityType = entityType;
        this.attributes = attributes;
    }

    public Entity(EntityId name, EntityType entityType, Map<String, Object> attributes, long entityTypeId) {
        this(name, entityType, attributes);
        this.entityTypeId = entityTypeId;
        this.deleted = false;
    }

    public Entity(EntityId name, EntityType entityType, Map<String, Object> attributes, long entityTypeId, boolean deleted) {
        this(name, entityType, attributes);
        this.entityTypeId = entityTypeId;
        this.deleted = deleted;
    }

    public Entity() {
    }

    public Entity(EntityId entityName, long entityType) {
        this.name = entityName;
        this.entityTypeId = entityType;
    }

    public EntityId getName() {
        return name;
    }

    public void setName(EntityId name) {
        this.name = name;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    @JsonGetter("entityType")
    public String getEntityTypeName() {
        return entityType.getName();
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public Long getEntityTypeId() {
        return entityTypeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Entity)) return false;

        Entity entity = (Entity) o;

        if (!getName().equals(entity.getName())) return false;
        return getEntityTypeId().equals(entity.getEntityTypeId());
    }

    @Override
    public int hashCode() {
        int result = getName().hashCode();
        result = 31 * result + getEntityTypeId().hashCode();
        return result;
    }
}
