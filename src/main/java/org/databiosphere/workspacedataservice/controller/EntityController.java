package org.databiosphere.workspacedataservice.controller;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.dao.EntityDao;
import org.databiosphere.workspacedataservice.service.EntityReferenceService;
import org.databiosphere.workspacedataservice.service.model.AttemptToUpsertDeletedEntity;
import org.databiosphere.workspacedataservice.service.model.EntityReferenceAction;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

@RestController
public class EntityController {

    private final EntityReferenceService referenceService;

    private final EntityDao dao;

    public EntityController(EntityReferenceService referenceService, EntityDao dao) {
        this.referenceService = referenceService;
        this.dao = dao;
    }

    private UUID getWorkspaceId(String wsNamespace, String wsName) {
        try {
            return dao.getWorkspaceId(wsNamespace, wsName);
        } catch (EmptyResultDataAccessException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Workspace not found");
        }
    }

    @PatchMapping("/")

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


}
