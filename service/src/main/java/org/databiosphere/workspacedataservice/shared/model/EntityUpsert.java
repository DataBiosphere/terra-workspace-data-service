package org.databiosphere.workspacedataservice.shared.model;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;

import java.util.List;

public class EntityUpsert {

    private EntityId name;

    private String entityType;

    private List<UpsertOperation> operations;

    public EntityId getName() {
        return name;
    }

    public void setName(EntityId name) {
        this.name = name;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public List<UpsertOperation> getOperations() {
        return operations;
    }

    @JsonSetter(nulls = Nulls.AS_EMPTY)
    public void setOperations(List<UpsertOperation> operations) {
        this.operations = operations;
    }
}
