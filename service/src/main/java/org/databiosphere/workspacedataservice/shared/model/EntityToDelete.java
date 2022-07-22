package org.databiosphere.workspacedataservice.shared.model;


public class EntityToDelete {

    private final String name;

    private final long entityTypeId;

    public EntityToDelete(String name, long entityTypeId) {
        this.name = name;
        this.entityTypeId = entityTypeId;
    }

    public String getName() {
        return name;
    }

    public long getEntityTypeId() {
        return entityTypeId;
    }
}
