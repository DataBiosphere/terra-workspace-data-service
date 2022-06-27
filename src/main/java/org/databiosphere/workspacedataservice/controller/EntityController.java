package org.databiosphere.workspacedataservice.controller;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.DataTypeInferer;
import org.databiosphere.workspacedataservice.dao.EntityDao;
import org.databiosphere.workspacedataservice.dao.SingleTenantDao;
import org.databiosphere.workspacedataservice.service.EntityReferenceService;
import org.databiosphere.workspacedataservice.service.model.AttemptToUpsertDeletedEntity;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.EntityReferenceAction;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@RestController
public class EntityController {

    private final EntityReferenceService referenceService;

    private final EntityDao dao;

    private final SingleTenantDao singleTenantDao;

    public EntityController(EntityReferenceService referenceService, EntityDao dao, SingleTenantDao singleTenantDao) {
        this.referenceService = referenceService;
        this.dao = dao;
        this.singleTenantDao = singleTenantDao;
    }

    private UUID getWorkspaceId(String wsNamespace, String wsName) {
        try {
            return dao.getWorkspaceId(wsNamespace, wsName);
        } catch (EmptyResultDataAccessException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Workspace not found");
        }
    }


    @PostMapping("/api/workspaces/{workspaceNamespace}/{workspaceName}/entities/batchUpsert")
    public ResponseEntity<String> batchUpsert(@PathVariable("workspaceNamespace") String wsNamespace,
                                              @PathVariable("workspaceName") String wsName,
                                              @RequestBody List<EntityUpsert> entitiesToUpdate){
        UUID workspaceId = getWorkspaceId(wsNamespace, wsName);
        Map<String, Map<String, Entity>> entitiesForUpsert;
        try {
            entitiesForUpsert = referenceService.convertToUpdatedEntities(entitiesToUpdate, workspaceId);
        } catch (AttemptToUpsertDeletedEntity e) {
            return new ResponseEntity<>(HttpStatus.CONFLICT);
        }
        EntityReferenceAction entityReferenceAction = referenceService.manageReferences(workspaceId, entitiesForUpsert);
        referenceService.saveReferencesAndEntities(entityReferenceAction);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @PostMapping("/st/api/workspaces/{workspaceId}/entities/batchUpsert")
    public ResponseEntity<String> batchUpsert(@PathVariable("workspaceId") UUID workspaceId,
                                              @RequestBody List<EntityUpsert> entitiesToUpdate){
        if(!singleTenantDao.workspaceSchemaExists(workspaceId)){
            singleTenantDao.createSchema(workspaceId);
        }
        ArrayListMultimap<String, EntityUpsert> eUpsertsByType = ArrayListMultimap.create();
        entitiesToUpdate.forEach(e -> eUpsertsByType.put(e.getEntityType(), e));
        Set<String> entityTypes = eUpsertsByType.keySet();
        DataTypeInferer dataTypeInferer = new DataTypeInferer();
        Map<String, Map<String, DataTypeMapping>> schemas = dataTypeInferer.inferTypes(entitiesToUpdate);
        for (String entityType : entityTypes) {
            Map<String, DataTypeMapping> schema = schemas.get(entityType);
            LinkedHashMap<String, DataTypeMapping> inferredOrderedSchema = new LinkedHashMap<>(schema);
            List<EntityUpsert> upsertsForType = eUpsertsByType.get(entityType);
            if(!singleTenantDao.entityTypeExists(workspaceId, entityType)){
                createEntityTypeAndAddEntities(workspaceId, entityType, inferredOrderedSchema, upsertsForType);
            } else {
                //table exists, add columns if needed
                Map<String, DataTypeMapping> existingTableSchema = addOrUpdateColumnIfNeeded(workspaceId, entityType, schema, singleTenantDao.getExistingTableSchema(workspaceId, entityType));
                Set<String> existingEntityNames = singleTenantDao.getEntityNames(workspaceId, entityType);
                //need to divide up between inserts and updates
                List<Entity> forUpdates = new ArrayList<>();
                List<Entity> forInsert = new ArrayList<>();
                for (EntityUpsert entityUpsert : upsertsForType) {
                    Entity entity = new Entity(entityUpsert.getName(), new EntityType(entityType), new HashMap<>());
                    updateAttributesForEntity(entityUpsert, entity, existingTableSchema);
                    if(existingEntityNames.contains(entityUpsert.getName())){
                        forUpdates.add(entity);
                    } else {
                        forInsert.add(entity);
                    }
                }
                LinkedHashMap<String, DataTypeMapping> orderedFullSchema = new LinkedHashMap<>(existingTableSchema);
                if(forInsert.size() > 0){
                    singleTenantDao.insertEntities(workspaceId, entityType, forInsert, orderedFullSchema);
                }
                if(forUpdates.size() > 0){
                    singleTenantDao.updateEntities(workspaceId, entityType, forUpdates, orderedFullSchema);
                }
            }
        }
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }


    private Map<String, DataTypeMapping> addOrUpdateColumnIfNeeded(UUID workspaceId, String entityType, Map<String, DataTypeMapping> schema, Map<String, DataTypeMapping> existingTableSchema) {
        MapDifference<String, DataTypeMapping> difference = Maps.difference(existingTableSchema, schema);
        Map<String, DataTypeMapping> colsToAdd = difference.entriesOnlyOnRight();
        for (String col : colsToAdd.keySet()) {
            singleTenantDao.addColumn(workspaceId, entityType, col, colsToAdd.get(col));
            existingTableSchema.put(col, colsToAdd.get(col));
        }
        Map<String, MapDifference.ValueDifference<DataTypeMapping>> differenceMap = difference.entriesDiffering();
        DataTypeInferer inferer = new DataTypeInferer();
        for (String column : differenceMap.keySet()) {
            MapDifference.ValueDifference<DataTypeMapping> valueDifference = differenceMap.get(column);
            DataTypeMapping updatedColType = inferer.selectBestType(valueDifference.leftValue(), valueDifference.rightValue());
            singleTenantDao.changeColumn(workspaceId, entityType, column, updatedColType);
            existingTableSchema.put(column, updatedColType);
        }
        return existingTableSchema;
    }

    private void createEntityTypeAndAddEntities(UUID workspaceId, String entityType, LinkedHashMap<String, DataTypeMapping> schema, List<EntityUpsert> upsertsForType) {
        singleTenantDao.createEntityType(workspaceId, schema, entityType);
        schema.put("name", DataTypeMapping.STRING);
        schema.put("all_attribute_values", DataTypeMapping.STRING);
        singleTenantDao.insertEntities(workspaceId, entityType, convertToEntities(upsertsForType, entityType, schema), schema);
    }

    private List<Entity> convertToEntities(List<EntityUpsert> entityUpserts, String entityType, Map<String, DataTypeMapping> schema) {
        List<Entity> result = new ArrayList<>();
        for (EntityUpsert entityUpsert : entityUpserts) {
            Entity entity = new Entity(entityUpsert.getName(), new EntityType(entityType), new HashMap<>());
            updateAttributesForEntity(entityUpsert, entity, schema);
            result.add(entity);
        }
        return result;
    }

    private void updateAttributesForEntity(EntityUpsert entityUpsert, Entity entity, Map<String, DataTypeMapping> schema){
        for (UpsertOperation operation : entityUpsert.getOperations()) {
            if(operation.getOp() == UpsertAction.AddUpdateAttribute){
                DataTypeMapping dataTypeMapping = schema.get(operation.getAttributeName());
                entity.getAttributes().put(operation.getAttributeName(), convertToType(operation.getAddUpdateAttribute(), dataTypeMapping));
            } else if (operation.getOp() == UpsertAction.RemoveAttribute){
                entity.getAttributes().put(operation.getAttributeName(), null);
            }
        }
    }

    private Object convertToType(Object val, DataTypeMapping typeMapping) {
        return switch (typeMapping){
            case DATE -> LocalDate.parse(val.toString(), DateTimeFormatter.ISO_LOCAL_DATE);
            case DATE_TIME -> LocalDateTime.parse(val.toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            case LONG, DOUBLE, STRING, JSON -> val;
        };
    }

    @PostMapping("/api/workspaces/{workspaceNamespace}/{workspaceName}/entities/delete")
    public ResponseEntity<String> deleteEntities(@PathVariable("workspaceNamespace") String wsNamespace,
                                                 @PathVariable("workspaceName") String wsName, @RequestBody List<Map<String, String>> entitiesToDelete){
        ArrayListMultimap<String, String> entitiesByType = ArrayListMultimap.create();
        entitiesToDelete.forEach(e -> entitiesByType.put(e.get("entityType"), e.get("entityName")));
        UUID workspaceId = getWorkspaceId(wsNamespace, wsName);
        List<EntityToDelete> entitiesInDb = new ArrayList<>();
        for(String entityTypeName: entitiesByType.keySet()){
            Long entityTypeId = dao.getEntityTypeId(workspaceId, entityTypeName);
            List<String> entities = entitiesByType.get(entityTypeName);
            entitiesInDb.addAll(dao.getEntitiesToDelete(entityTypeId, new HashSet<>(entities), entityTypeName, true));
            if(dao.areEntitiesReferenced(entityTypeId, entities)){
                return new ResponseEntity<>("Can't delete referenced entities", HttpStatus.CONFLICT);
            }
        }
        if(entitiesInDb.size() != entitiesToDelete.size()){
            return new ResponseEntity<>("Not all entities exist", HttpStatus.BAD_REQUEST);
        }

        dao.deleteEntities(entitiesInDb);

        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @GetMapping("/api/workspaces/{workspaceNamespace}/{workspaceName}/entityQuery/{entityType}")
    public EntityQueryResult queryForEntities(@PathVariable("workspaceNamespace") String wsNamespace,
                                              @PathVariable("workspaceName") String wsName,
                                              @PathVariable("entityType") String entityType,
                                              @RequestParam(defaultValue = "1") int page,
                                              @RequestParam(defaultValue = "10") int pageSize,
                                              @RequestParam(defaultValue = "name") String sortField,
                                              @RequestParam(defaultValue = "asc") String sortDirection,
                                              @RequestParam(defaultValue = "") String filterTerms,
                                              @RequestParam(required = false) List<String> fields) {
        Preconditions.checkArgument(Set.of("asc", "desc").contains(sortDirection.toLowerCase(Locale.ROOT)));
        EntityQueryParameters queryParameters = new EntityQueryParameters(page, pageSize, sortField, sortDirection, filterTerms);
        UUID workspaceId = getWorkspaceId(wsNamespace, wsName);
        Long entityTypeId = dao.getEntityTypeId(workspaceId, entityType);
        int totalEntityCount = dao.getTotalEntityCount(entityTypeId);
        int filteredEntityCount = StringUtils.isNotBlank(filterTerms) ? dao.getFilteredEntityCount(entityTypeId, filterTerms) : totalEntityCount;
        EntityQueryResultMetadata entityQueryResultMetadata = new EntityQueryResultMetadata(totalEntityCount, filteredEntityCount, (int) Math.ceil(filteredEntityCount / (double) pageSize));
        return new EntityQueryResult(queryParameters, entityQueryResultMetadata, filteredEntityCount > 0 ? dao.getSelectedEntities(entityTypeId, pageSize,
                (page-1) * pageSize, filterTerms, sortField, sortDirection, fields) : Collections.emptyList());
    }

    @GetMapping("/st/api/workspaces/{workspaceId}/entityQuery/{entityType}")
    public EntityQueryResult queryForEntities(@PathVariable("workspaceId") UUID workspaceId,
                                              @PathVariable("entityType") String entityType,
                                              @RequestParam(defaultValue = "1") int page,
                                              @RequestParam(defaultValue = "10") int pageSize,
                                              @RequestParam(defaultValue = "name") String sortField,
                                              @RequestParam(defaultValue = "asc") String sortDirection,
                                              @RequestParam(defaultValue = "") String filterCol,
                                              @RequestParam(defaultValue = "") String filterTerms,
                                              @RequestParam(required = false) List<String> fields) {
        Preconditions.checkArgument(Set.of("asc", "desc").contains(sortDirection.toLowerCase(Locale.ROOT)));
        EntityQueryParameters queryParameters = new EntityQueryParameters(page, pageSize, sortField, sortDirection, filterTerms);
        int totalEntityCount = singleTenantDao.getEntityCount(entityType, workspaceId);
        int filteredEntityCount = StringUtils.isNotBlank(filterTerms) ? singleTenantDao.getFilteredEntityCount(workspaceId, entityType, filterCol, filterTerms) : totalEntityCount;
        EntityQueryResultMetadata entityQueryResultMetadata = new EntityQueryResultMetadata(totalEntityCount, filteredEntityCount, (int) Math.ceil(filteredEntityCount / (double) pageSize));
//        return new EntityQueryResult(queryParameters, entityQueryResultMetadata, filteredEntityCount > 0 ? singleTenantDao.getSelectedEntities(entityType, pageSize,
//                (page-1) * pageSize, filterTerms, sortField, sortDirection, fields) : Collections.emptyList());
        return null;
    }


}
