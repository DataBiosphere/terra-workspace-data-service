package org.databiosphere.workspacedataservice.service;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Sets;
import org.databiosphere.workspacedataservice.dao.EntityDao;
import org.databiosphere.workspacedataservice.service.model.AttemptToUpsertDeletedEntity;
import org.databiosphere.workspacedataservice.service.model.EntityReference;
import org.databiosphere.workspacedataservice.service.model.EntityReferenceAction;
import org.databiosphere.workspacedataservice.service.model.InvalidEntityReference;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class EntityReferenceService {

    private static final String ENTITY_TYPE_KEY = "entityType";
    private static final String ENTITY_NAME_KEY = "entityName";

    private final EntityDao dao;

    public EntityReferenceService(EntityDao dao) {
        this.dao = dao;
    }

    private Map<EntityId, Entity> handleNewEntityType(UUID instanceId, List<EntityUpsert> entityUpserts, String entityTypeName) throws AttemptToUpsertDeletedEntity {
        EntityType entityType = new EntityType(entityTypeName);
        long newEntityTypeId = dao.loadEntityType(entityType, instanceId);
        return applyEntityUpserts(entityUpserts, newEntityTypeId, new HashMap<>());
    }

    private Map<EntityId, Entity> applyEntityUpserts(List<EntityUpsert> entityUpserts, long entityTypeId, Map<EntityId, Entity> entitiesByName) throws AttemptToUpsertDeletedEntity {
        for (EntityUpsert entityUpsert : entityUpserts) {
            EntityId entityName = entityUpsert.getName();
            if(entitiesByName.containsKey(entityName)){
                Entity entity = entitiesByName.get(entityName);
                if(entity.getDeleted()){
                    throw new AttemptToUpsertDeletedEntity("Entity " + entityName + " was deleted previously");
                }
                updateAttributesForEntity(entityUpsert, entity);
            } else {
                Entity entity = new Entity(entityName, new EntityType(entityUpsert.getEntityType()), new EntityAttributes(new HashMap<>()), entityTypeId);
                updateAttributesForEntity(entityUpsert, entity);
                entitiesByName.put(entityName, entity);
            }
        }
        return entitiesByName;
    }

    private void updateAttributesForEntity(EntityUpsert entityUpsert, Entity entity){
        for (UpsertOperation operation : entityUpsert.getOperations()) {
            if(operation.getOp() == UpsertAction.AddUpdateAttribute){
                entity.getAttributes().put(operation.getAttributeName(), operation.getAddUpdateAttribute());
            } else if (operation.getOp() == UpsertAction.RemoveAttribute){
                entity.getAttributes().remove(operation.getAttributeName());
            }
        }
    }

    public List<EntityReference> getEntityReferences(List<Entity> entities, UUID instanceId) {
        List<EntityReference> result = new ArrayList<>();
        for (Entity entity : entities) {
            List<Map> referencedAttrs = entity.getAttributes().values().stream().filter(Map.class::isInstance).map(Map.class::cast)
                    .filter(map -> map.size() == 2 && map.containsKey(ENTITY_TYPE_KEY) && map.containsKey(ENTITY_NAME_KEY)).collect(Collectors.toList());
            for (Map referencedAttr : referencedAttrs) {
                result.add(new EntityReference(entity.getName(), entity.getEntityTypeId(),
                        dao.getEntityTypeId(instanceId, referencedAttr.get(ENTITY_TYPE_KEY).toString()),
                        (EntityId)referencedAttr.get(ENTITY_NAME_KEY)));
            }
        }
        return result;
    }


    public EntityReferenceAction manageSingleEntityReference(UUID instanceId, Entity entity) {
        List<EntityReference> referencesToAdd = new ArrayList<>();
        List<EntityReference> referencesToRemove = new ArrayList<>();
        List<EntityReference> updatedReferences = getEntityReferences(Collections.singletonList(entity), instanceId);
        List<EntityReference> referencesForEntity = dao.getReferencesForEntities(Collections.singletonList(entity), entity.getEntityTypeId());
        calculateReferenceOpsForEntityType(referencesToAdd, referencesToRemove, updatedReferences, referencesForEntity);
        return new EntityReferenceAction(referencesToAdd, referencesToRemove, Collections.singletonList(entity));
    }

    private void calculateReferenceOpsForEntityType(List<EntityReference> referencesToAdd, List<EntityReference> referencesToRemove, List<EntityReference> updatedReferences, List<EntityReference> referencesForEntity) {
        HashSet<EntityReference> existingRefsSet = new HashSet<>(referencesForEntity);
        HashSet<EntityReference> updateRefsSet = new HashSet<>(updatedReferences);
        if (!updateRefsSet.equals(existingRefsSet)) {
            //if existing has elements not contained in updated we need to remove
            referencesToRemove.addAll(Sets.difference(existingRefsSet, updateRefsSet));
            //if updated has elements not contained in existing we need to add
            referencesToAdd.addAll(Sets.difference(updateRefsSet, existingRefsSet));
        }
    }

    public EntityReferenceAction manageReferences(UUID instanceId, Map<String, Map<EntityId, Entity>> entitiesForUpsert) {
        List<EntityReference> referencesToAdd = new ArrayList<>();
        List<EntityReference> referencesToRemove = new ArrayList<>();
        for (String entityType : entitiesForUpsert.keySet()) {
            ArrayList<Entity> entitiesForType = new ArrayList<>(entitiesForUpsert.get(entityType).values());
            List<EntityReference> updatedReferences = getEntityReferences(entitiesForType, instanceId);
            List<EntityReference> existingReferences = dao.getReferencesForEntities(entitiesForType, dao.getEntityTypeId(instanceId, entityType));
            calculateReferenceOpsForEntityType(referencesToAdd, referencesToRemove, updatedReferences, existingReferences);
        }
        return new EntityReferenceAction(referencesToAdd, referencesToRemove,
                entitiesForUpsert.values().stream().flatMap(s -> s.values().stream()).collect(Collectors.toList()));
    }

    public Map<String, Map<EntityId, Entity>> convertToUpdatedEntities(List<EntityUpsert> entitiesToUpdate, UUID instanceId) throws AttemptToUpsertDeletedEntity {
        ArrayListMultimap<String, EntityUpsert> eUpsertsByType = ArrayListMultimap.create();
        entitiesToUpdate.forEach(e -> eUpsertsByType.put(e.getEntityType(), e));
        Map<String, Map<EntityId, Entity>> entitiesByTypeAndName = new HashMap<>();
        for (String entityTypeName : eUpsertsByType.keySet()) {
            Long entityTypeId = dao.getEntityTypeId(instanceId, entityTypeName);
            if(entityTypeId == -1){
                entitiesByTypeAndName.put(entityTypeName, handleNewEntityType(instanceId, eUpsertsByType.get(entityTypeName), entityTypeName));
            } else {
                entitiesByTypeAndName.put(entityTypeName, handleExistingEntityType(eUpsertsByType.get(entityTypeName), entityTypeId, entityTypeName));
            }
        }
        return entitiesByTypeAndName;
    }

    public List<Entity> getEntities(long entityTypeId, String entityTypeName, Set<EntityId> entityNames, boolean excludeDeletedEntities) {
        return dao.getNamedEntities(entityTypeId, entityNames, entityTypeName, excludeDeletedEntities);
    }

    private Map<EntityId, Entity> handleExistingEntityType(List<EntityUpsert> entityUpserts, long entityTypeId,
                                                           String entityTypeName) throws AttemptToUpsertDeletedEntity {
        Set<EntityId> entityNames = entityUpserts.stream().map(EntityUpsert::getName).collect(Collectors.toSet());
        List<Entity> existingEntities = getEntities(entityTypeId, entityTypeName, entityNames, false);
        Map<EntityId, Entity> entitiesByName = new HashMap<>();
        existingEntities.forEach(e -> entitiesByName.put(e.getName(), e));

        return applyEntityUpserts(entityUpserts, entityTypeId, entitiesByName);
    }

    public void saveReferencesAndEntities(EntityReferenceAction entityReferenceAction)  {
        if(entityReferenceAction.getOutOfBatchreferencesToAdd().size() > 0){
            try {
                dao.saveEntitiesReferenced(entityReferenceAction.getOutOfBatchreferencesToAdd());
            } catch (InvalidEntityReference e) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid entity references " + entityReferenceAction.getReferencesToRemove());
            }
        }
        if(entityReferenceAction.getReferencesToRemove().size() > 0){
            dao.removeReferences(entityReferenceAction.getReferencesToRemove());
        }
        dao.batchUpsert(entityReferenceAction.getUpsertBatch());
        if(entityReferenceAction.getInBatchreferencesToAdd().size() > 0){
            try {
                dao.saveEntitiesReferenced(entityReferenceAction.getInBatchreferencesToAdd());
            } catch (InvalidEntityReference e){
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid entity references " + entityReferenceAction.getInBatchreferencesToAdd());
            }
        }
    }
}
