package org.databiosphere.workspacedataservice.controller;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.DataTypeInferer;
import org.databiosphere.workspacedataservice.MissingReferencedTableException;
import org.databiosphere.workspacedataservice.dao.SingleTenantDao;
import org.databiosphere.workspacedataservice.service.SingleTenantEntityRefService;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.SingleTenantEntityReference;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@RestController
public class SingleTenantEntityController {

    private final SingleTenantDao singleTenantDao;

    private final SingleTenantEntityRefService referenceService;
    private final DataTypeInferer inferer = new DataTypeInferer();

    public SingleTenantEntityController(SingleTenantDao singleTenantDao, SingleTenantEntityRefService referenceService) {
        this.singleTenantDao = singleTenantDao;
        this.referenceService = referenceService;
    }

    @GetMapping("/st/api/workspaces/{workspaceId}/entityQuery/{entityType}")
    public EntityQueryResult queryForEntities(@PathVariable("workspaceId") UUID workspaceId,
                                              @PathVariable("entityType") String entityType,
                                              @RequestParam(defaultValue = "1") int page,
                                              @RequestParam(defaultValue = "10") int pageSize,
                                              @RequestParam(defaultValue = "sys_name") String sortField,
                                              @RequestParam(defaultValue = "asc") String sortDirection,
                                              @RequestParam(defaultValue = "") String filterTerms,
                                              @RequestParam(required = false) List<String> fields) {
        if(!singleTenantDao.entityTypeExists(workspaceId, entityType)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Could not find entity type " + entityType);
        }
            Preconditions.checkArgument(Set.of("asc", "desc").contains(sortDirection.toLowerCase(Locale.ROOT)));
        EntityQueryParameters queryParameters = new EntityQueryParameters(page, pageSize, sortField, sortDirection, filterTerms);
        int totalEntityCount = singleTenantDao.getEntityCount(entityType, workspaceId);
        Map<String, DataTypeMapping> schema = singleTenantDao.getExistingTableSchema(workspaceId, entityType);
        int filteredEntityCount = StringUtils.isNotBlank(filterTerms) ? singleTenantDao.getFilteredEntityCount(workspaceId, entityType, filterTerms, schema) : totalEntityCount;
        EntityQueryResultMetadata entityQueryResultMetadata = new EntityQueryResultMetadata(totalEntityCount, filteredEntityCount, (int) Math.ceil(filteredEntityCount / (double) pageSize));
        return new EntityQueryResult(queryParameters, entityQueryResultMetadata, filteredEntityCount > 0 ? singleTenantDao.getSelectedEntities(entityType, pageSize,
                (page-1) * pageSize, filterTerms, sortField, sortDirection, fields, workspaceId, schema, singleTenantDao.getReferenceCols(workspaceId, entityType)) : Collections.emptyList());
    }

    private void updateAttributesForEntity(EntityUpsert entityUpsert, Entity entity, Map<String, DataTypeMapping> schema){
        for (UpsertOperation operation : entityUpsert.getOperations()) {
            Map<String, Object> attributes = entity.getAttributes().getAttributes();
            if(operation.getOp() == UpsertAction.AddUpdateAttribute){
                DataTypeMapping dataTypeMapping = schema.get(operation.getAttributeName());
                attributes.put(operation.getAttributeName(), inferer.convertToType(operation.getAddUpdateAttribute(), dataTypeMapping));
                entity.setAttributes(new EntityAttributes(attributes));
            } else if (operation.getOp() == UpsertAction.RemoveAttribute){
                attributes.put(operation.getAttributeName(), null);
                entity.setAttributes(new EntityAttributes(attributes));
            }
        }
    }

    private List<Entity> convertToEntities(List<EntityUpsert> entityUpserts, String entityType, Map<String, DataTypeMapping> schema) {
        List<Entity> result = new ArrayList<>();
        for (EntityUpsert entityUpsert : entityUpserts) {
            Entity entity = new Entity(entityUpsert.getName(), new EntityType(entityType), new EntityAttributes(new HashMap<>()));
            updateAttributesForEntity(entityUpsert, entity, schema);
            result.add(entity);
        }
        return result;
    }

    private void createEntityTypeAndAddEntities(UUID workspaceId, String entityType, LinkedHashMap<String, DataTypeMapping> schema, List<EntityUpsert> upsertsForType) {
        List<Entity> entities = convertToEntities(upsertsForType, entityType, schema);
        Set<SingleTenantEntityReference> entityReferences = referenceService.attachRefValueToEntitiesAndFindReferenceColumns(entities);
        try {
            singleTenantDao.createEntityType(workspaceId, schema, entityType, entityReferences);
        } catch (MissingReferencedTableException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "It looks like you're attempting to assign a reference " +
                    "to a table that does not exist");
        }
        singleTenantDao.batchUpsert(workspaceId, entityType, entities, schema);
    }

    private void addOrUpdateColumnIfNeeded(UUID workspaceId, String entityType, Map<String, DataTypeMapping> schema, Map<String,
            DataTypeMapping> existingTableSchema, List<Entity> entities) {
        MapDifference<String, DataTypeMapping> difference = Maps.difference(existingTableSchema, schema);
        Map<String, DataTypeMapping> colsToAdd = difference.entriesOnlyOnRight();
        Set<SingleTenantEntityReference> references = referenceService.attachRefValueToEntitiesAndFindReferenceColumns(entities);
        Map<String, List<SingleTenantEntityReference>> newRefCols = references.stream().collect(Collectors.groupingBy(SingleTenantEntityReference::getReferenceColName));
        //TODO: better communicate to the user that they're trying to assign multiple entity types to a single column
        Preconditions.checkArgument(newRefCols.values().stream().filter(l -> l.size() > 1).findAny().isEmpty());
        for (String col : colsToAdd.keySet()) {
            singleTenantDao.addColumn(workspaceId, entityType, col, colsToAdd.get(col));
            if(newRefCols.containsKey(col)) {
                String referencedEntityType = null;
                try {
                    referencedEntityType = newRefCols.get(col).get(0).getReferencedEntityType().getName();
                    singleTenantDao.addForeignKeyForReference(entityType, referencedEntityType, workspaceId, col);
                } catch (MissingReferencedTableException e) {
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "It looks like you're attempting to assign a reference " +
                            "to a table, " + referencedEntityType + ", that does not exist");
                }
            }
        }
        if(!singleTenantDao.getReferenceCols(workspaceId, entityType).stream().map(SingleTenantEntityReference::getReferenceColName)
                .collect(Collectors.toSet()).containsAll(newRefCols.keySet())){
            throw new ResponseStatusException(HttpStatus.CONFLICT, "It looks like you're attempting to assign a reference " +
                    "to an existing column that was not configured for references");
        }
        Map<String, MapDifference.ValueDifference<DataTypeMapping>> differenceMap = difference.entriesDiffering();
        for (String column : differenceMap.keySet()) {
            MapDifference.ValueDifference<DataTypeMapping> valueDifference = differenceMap.get(column);
            if(valueDifference.rightValue() == DataTypeMapping.FOR_ATTRIBUTE_DEL){
                continue;
            }
            DataTypeMapping updatedColType = inferer.selectBestType(valueDifference.leftValue(), valueDifference.rightValue());
            singleTenantDao.changeColumn(workspaceId, entityType, column, updatedColType);
        }
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
        Map<String, LinkedHashMap<String, DataTypeMapping>> schemas = inferer.inferTypes(entitiesToUpdate);
        for (String entityType : entityTypes) {
            LinkedHashMap<String, DataTypeMapping> columnsModifiedSchema = schemas.get(entityType) == null ? new LinkedHashMap<>() : schemas.get(entityType);
            List<EntityUpsert> upsertsForType = eUpsertsByType.get(entityType);
            if(!singleTenantDao.entityTypeExists(workspaceId, entityType)){
                createEntityTypeAndAddEntities(workspaceId, entityType, columnsModifiedSchema, upsertsForType);
            } else {
                //table exists, add columns if needed
                List<Entity> entities = convertToEntities(upsertsForType, entityType, columnsModifiedSchema);
                addOrUpdateColumnIfNeeded(workspaceId, entityType, columnsModifiedSchema,
                        singleTenantDao.getExistingTableSchema(workspaceId, entityType), entities);
                singleTenantDao.batchUpsert(workspaceId, entityType, entities, columnsModifiedSchema);            }
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
            return new ResponseEntity<>("Not all entities exist", HttpStatus.NOT_FOUND);
        }

        for(String entityTypeName: entitiesByType.keySet()){
            try {
                singleTenantDao.deleteEntities(entityTypeName,  entitiesByType.get(entityTypeName), workspaceId);
            } catch (DataIntegrityViolationException e) {
                return new ResponseEntity<>("Can't delete referenced entities", HttpStatus.BAD_REQUEST);
            }
        }
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

}
