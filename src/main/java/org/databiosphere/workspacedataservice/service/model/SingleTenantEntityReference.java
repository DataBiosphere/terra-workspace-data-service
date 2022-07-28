package org.databiosphere.workspacedataservice.service.model;

import org.databiosphere.workspacedataservice.shared.model.EntityId;
import org.databiosphere.workspacedataservice.shared.model.EntityType;

public class SingleTenantEntityReference {

    private final EntityId entityName;

    private final EntityType entityType;

    private final EntityType referencedEntityType;

    private final EntityId referencedEntityName;

    public SingleTenantEntityReference(EntityId entityName, EntityType entityType, EntityType referencedEntityType, EntityId referencedEntityName) {
        this.entityName = entityName;
        this.entityType = entityType;
        this.referencedEntityType = referencedEntityType;
        this.referencedEntityName = referencedEntityName;
    }

    public EntityId getEntityName() {
        return entityName;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public EntityType getReferencedEntityType() {
        return referencedEntityType;
    }

    public EntityId getReferencedEntityName() {
        return referencedEntityName;
    }
}
