package org.databiosphere.workspacedataservice.controller;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.DataTypeInferer;
import org.databiosphere.workspacedataservice.dao.SingleTenantDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@RestController
public class SingleTenantEntityController {

    private final SingleTenantDao singleTenantDao;

    public SingleTenantEntityController(SingleTenantDao singleTenantDao) {
        this.singleTenantDao = singleTenantDao;
    }

    @GetMapping("/st/api/workspaces/{workspaceId}/entityQuery/{entityType}")
    public EntityQueryResult queryForEntities(@PathVariable("workspaceId") UUID workspaceId,
                                              @PathVariable("entityType") String entityType,
                                              @RequestParam(defaultValue = "1") int page,
                                              @RequestParam(defaultValue = "10") int pageSize,
                                              @RequestParam(defaultValue = "name") String sortField,
                                              @RequestParam(defaultValue = "asc") String sortDirection,
                                              @RequestParam(defaultValue = "") String filterTerms,
                                              @RequestParam(required = false) List<String> fields) {
        if(!singleTenantDao.entityTypeExists(workspaceId, entityType)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Could not find entity type " + entityType);
        }
            Preconditions.checkArgument(Set.of("asc", "desc").contains(sortDirection.toLowerCase(Locale.ROOT)));
        EntityQueryParameters queryParameters = new EntityQueryParameters(page, pageSize, sortField, sortDirection, filterTerms);
        int totalEntityCount = singleTenantDao.getEntityCount(entityType, workspaceId);
        int filteredEntityCount = StringUtils.isNotBlank(filterTerms) ? singleTenantDao.getFilteredEntityCount(workspaceId, entityType, filterTerms) : totalEntityCount;
        EntityQueryResultMetadata entityQueryResultMetadata = new EntityQueryResultMetadata(totalEntityCount, filteredEntityCount, (int) Math.ceil(filteredEntityCount / (double) pageSize));
        return new EntityQueryResult(queryParameters, entityQueryResultMetadata, filteredEntityCount > 0 ? singleTenantDao.getSelectedEntities(entityType, pageSize,
                (page-1) * pageSize, filterTerms, sortField, sortDirection, fields, workspaceId) : Collections.emptyList());
    }

    private Object convertToType(Object val, DataTypeMapping typeMapping) {
        return switch (typeMapping){
            case DATE -> LocalDate.parse(val.toString(), DateTimeFormatter.ISO_LOCAL_DATE);
            case DATE_TIME -> LocalDateTime.parse(val.toString(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            case LONG, DOUBLE, STRING, JSON -> val;
        };
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

    private List<Entity> convertToEntities(List<EntityUpsert> entityUpserts, String entityType, Map<String, DataTypeMapping> schema) {
        List<Entity> result = new ArrayList<>();
        for (EntityUpsert entityUpsert : entityUpserts) {
            Entity entity = new Entity(entityUpsert.getName(), new EntityType(entityType), new HashMap<>());
            updateAttributesForEntity(entityUpsert, entity, schema);
            result.add(entity);
        }
        return result;
    }

    private void createEntityTypeAndAddEntities(UUID workspaceId, String entityType, LinkedHashMap<String, DataTypeMapping> schema, List<EntityUpsert> upsertsForType) {
        singleTenantDao.createEntityType(workspaceId, schema, entityType);
        schema.put("name", DataTypeMapping.STRING);
        schema.put("all_attribute_values", DataTypeMapping.STRING);
        singleTenantDao.insertEntities(workspaceId, entityType, convertToEntities(upsertsForType, entityType, schema), schema);
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

    @PostMapping("/st/api/workspaces/{workspaceId}/entities/delete")
    public ResponseEntity<String> deleteEntities(@PathVariable("workspaceId") UUID workspaceId, @RequestBody List<Map<String, String>> entitiesToDelete){
        ArrayListMultimap<String, String> entitiesByType = ArrayListMultimap.create();
        entitiesToDelete.forEach(e -> entitiesByType.put(e.get("entityType"), e.get("entityName")));

        int entitiesInDbCount = 0;
        for (String entityTypeName: entitiesByType.keySet()){
            if(!singleTenantDao.entityTypeExists(workspaceId, entityTypeName)){
                return new ResponseEntity<>("Entity type does not exist", HttpStatus.NOT_FOUND);
            }
            entitiesInDbCount += singleTenantDao.getEntityCount(entityTypeName,  entitiesByType.get(entityTypeName), workspaceId);
        }

        if(entitiesInDbCount != entitiesToDelete.size()){
            return new ResponseEntity<>("Not all entities exist", HttpStatus.BAD_REQUEST);
        }

        for(String entityTypeName: entitiesByType.keySet()){
            singleTenantDao.deleteEntities(entityTypeName,  entitiesByType.get(entityTypeName), workspaceId);
        }
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

}
