package org.databiosphere.workspacedataservice.controller;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.databiosphere.workspacedataservice.dao.SingleTenantDao;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.SingleTenantEntityRefService;
import org.databiosphere.workspacedataservice.service.model.*;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;
import java.util.stream.Collectors;

@RestController
public class EntityController {

    private final SingleTenantEntityRefService referenceService;

    private final SingleTenantDao singleTenantDao;
    private DataTypeInferer inferer;

    public EntityController(SingleTenantEntityRefService referenceService, SingleTenantDao singleTenantDao) {
        this.referenceService = referenceService;
        this.singleTenantDao = singleTenantDao;
        this.inferer = new DataTypeInferer();
    }

    @PatchMapping("/{instanceId}/entities/{version}/{entityType}/{entityId}")
    public ResponseEntity<EntityResponse> updateSingleEntity(@PathVariable("instanceId") UUID instanceId,
                                                             @PathVariable("version") String version,
                                                             @PathVariable("entityType") EntityType entityType,
                                                             @PathVariable("entityId") EntityId entityId,
                                                             @RequestBody EntityRequest entityRequest){
        Preconditions.checkArgument(version.equals("v0.2"));
        String entityTypeName = entityType.getName();
        Entity singleEntity = singleTenantDao.getSingleEntity(instanceId, entityType, entityId,
                singleTenantDao.getReferenceCols(instanceId, entityTypeName));
        if(singleEntity == null){
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Entity not found");
        }
        Map<String, Object> updatedAtts = entityRequest.entityAttributes().getAttributes();
        Map<String, DataTypeMapping> typeMapping = inferer.inferTypes(updatedAtts);
        //TODO: remove entityType/entityName JSON object format for references and move to URIs in the request/response payloads
        Map<String, DataTypeMapping> existingTableSchema = singleTenantDao.getExistingTableSchema(instanceId, entityTypeName);
        List<Entity> entities = Collections.singletonList(singleEntity);
        addOrUpdateColumnIfNeeded(instanceId, entityType.getName(), typeMapping, existingTableSchema, new ArrayList<>(entities));
        singleTenantDao.batchUpsert(instanceId, entityTypeName, entities, new LinkedHashMap<>(existingTableSchema));
        Entity updatedEntity = singleTenantDao.getSingleEntity(instanceId, entityType, entityId, singleTenantDao.getReferenceCols(instanceId, entityType.getName()));
        EntityResponse response = new EntityResponse(entityId, entityType, updatedEntity.getAttributes(),
                new EntityMetadata("TODO: SUPERFRESH"));
        return new ResponseEntity<>(response, HttpStatus.OK);
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


    @GetMapping("/{instanceId}/entities/{version}/{entityType}/{entityId}")
    public ResponseEntity<EntityResponse> getSingleEntity(@PathVariable("instanceId") UUID instanceId,
                                              @PathVariable("version") String version,
                                              @PathVariable("entityType") EntityType entityType,
                                              @PathVariable("entityId") EntityId entityId) {
        Preconditions.checkArgument(version.equals("v0.2"));
        Entity result = singleTenantDao.getSingleEntity(instanceId, entityType, entityId, singleTenantDao.getReferenceCols(instanceId, entityType.getName()));
        if (result == null){
            //TODO: standard exception classes
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Entity not found");
        }
        EntityResponse response = new EntityResponse(entityId, entityType, result.getAttributes(),
                new EntityMetadata("TODO: ENTITYMETADATA"));
        return new ResponseEntity<>(response, HttpStatus.OK);
    }


}
