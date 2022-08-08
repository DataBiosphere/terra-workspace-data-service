package org.databiosphere.workspacedataservice.service.model;

import org.databiosphere.workspacedataservice.shared.model.Entity;

import java.util.ArrayList;
import java.util.List;

public class EntityReferenceAction {

    private final List<EntityReference> outOfBatchreferencesToAdd = new ArrayList<>();
    private final List<EntityReference> inBatchreferencesToAdd = new ArrayList<>();
    private final List<EntityReference> referencesToRemove;
    private final List<Entity> upsertBatch;


    public EntityReferenceAction(List<EntityReference> referencesToAdd, List<EntityReference> referencesToRemove, List<Entity> upsertBatch) {
        this.referencesToRemove = referencesToRemove;
        this.upsertBatch = upsertBatch;
        this.init(referencesToAdd);
    }

    private void init(List<EntityReference> referencesToAdd) {
        for (EntityReference entityReference : referencesToAdd) {
            if(!upsertBatch.contains(new Entity(entityReference.getReferencedEntityName(), entityReference.getReferencedEntityType()))){
                outOfBatchreferencesToAdd.add(entityReference);
            } else {
                inBatchreferencesToAdd.add(entityReference);
            }
        }
    }

    public List<EntityReference> getOutOfBatchreferencesToAdd() {
        return outOfBatchreferencesToAdd;
    }

    public List<EntityReference> getInBatchreferencesToAdd() {
        return inBatchreferencesToAdd;
    }

    public List<EntityReference> getReferencesToRemove() {
        return referencesToRemove;
    }


    public List<Entity> getUpsertBatch() {
        return upsertBatch;
    }
}
