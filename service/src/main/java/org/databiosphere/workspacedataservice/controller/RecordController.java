package org.databiosphere.workspacedataservice.controller;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.*;
import java.util.stream.Collectors;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.*;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class RecordController {

  private final RecordDao recordDao;
  private final DataTypeInferer inferer;

  public RecordController(RecordDao recordDao) {
    this.recordDao = recordDao;
    this.inferer = new DataTypeInferer();
  }

  @PatchMapping("/{instanceId}/entities/{version}/{recordType}/{recordId}")
  public ResponseEntity<RecordResponse> updateSingleRecord(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("version") String version,
      @PathVariable("recordType") RecordType recordType,
      @PathVariable("recordId") RecordId recordId,
      @RequestBody RecordRequest recordRequest) {
    validateVersion(version);
    String recordTypeName = recordType.getName();
    Record singleRecord =
        recordDao.getSingleRecord(
            instanceId,
                recordType,
                recordId,
            recordDao.getReferenceCols(instanceId, recordTypeName));
    if (singleRecord == null) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Record not found");
    }
    Map<String, Object> updatedAtts = recordRequest.recordAttributes().getAttributes();
    Map<String, Object> allAttrs = new HashMap<>(singleRecord.getAttributes().getAttributes());
    allAttrs.putAll(updatedAtts);

    Map<String, DataTypeMapping> typeMapping = inferer.inferTypes(updatedAtts);
    Map<String, DataTypeMapping> existingTableSchema =
        recordDao.getExistingTableSchema(instanceId, recordTypeName);
    singleRecord.setAttributes(new RecordAttributes(allAttrs));
    List<Record> entities = Collections.singletonList(singleRecord);
    Map<String, DataTypeMapping> updatedSchema =
        addOrUpdateColumnIfNeeded(
            instanceId, recordType.getName(), typeMapping, existingTableSchema, entities);
    try {
      recordDao.batchUpsert(
          instanceId, recordTypeName, entities, new LinkedHashMap<>(updatedSchema));
      RecordResponse response =
          new RecordResponse(
                  recordId,
                  recordType,
              singleRecord.getAttributes(),
              new RecordMetadata("TODO: SUPERFRESH"));
      return new ResponseEntity<>(response, HttpStatus.OK);

    } catch (InvalidRelation e) {
      return new ResponseEntity(e.getMessage(), HttpStatus.BAD_REQUEST);
    }
  }

  private Map<String, DataTypeMapping> addOrUpdateColumnIfNeeded(
      UUID instanceId,
      String recordType,
      Map<String, DataTypeMapping> schema,
      Map<String, DataTypeMapping> existingTableSchema,
      List<Record> records) {
    MapDifference<String, DataTypeMapping> difference =
        Maps.difference(existingTableSchema, schema);
    Map<String, DataTypeMapping> colsToAdd = difference.entriesOnlyOnRight();
    Set<Relation> references = RelationUtils.findRelations(records);
    Map<String, List<Relation>> newRefCols =
        references.stream().collect(Collectors.groupingBy(Relation::referenceColName));
    // TODO: better communicate to the user that they're trying to assign multiple record types to a
    // single column
    Preconditions.checkArgument(
        newRefCols.values().stream().filter(l -> l.size() > 1).findAny().isEmpty());
    for (String col : colsToAdd.keySet()) {
      recordDao.addColumn(instanceId, recordType, col, colsToAdd.get(col));
      schema.put(col, colsToAdd.get(col));
      if (newRefCols.containsKey(col)) {
        String referencedRecordType = null;
        try {
          referencedRecordType = newRefCols.get(col).get(0).referencedRecordType().getName();
          recordDao.addForeignKeyForReference(recordType, referencedRecordType, instanceId, col);
        } catch (MissingReferencedTableException e) {
          throw new ResponseStatusException(
              HttpStatus.BAD_REQUEST,
              "It looks like you're attempting to assign a reference "
                  + "to a table, "
                  + referencedRecordType
                  + ", that does not exist");
        }
      }
    }
    if (!recordDao.getReferenceCols(instanceId, recordType).stream()
        .map(Relation::referenceColName)
        .collect(Collectors.toSet())
        .containsAll(newRefCols.keySet())) {
      throw new ResponseStatusException(
          HttpStatus.CONFLICT,
          "It looks like you're attempting to assign a reference "
              + "to an existing column that was not configured for references");
    }
    Map<String, MapDifference.ValueDifference<DataTypeMapping>> differenceMap =
        difference.entriesDiffering();
    for (String column : differenceMap.keySet()) {
      MapDifference.ValueDifference<DataTypeMapping> valueDifference = differenceMap.get(column);
      DataTypeMapping updatedColType =
          inferer.selectBestType(valueDifference.leftValue(), valueDifference.rightValue());
      recordDao.changeColumn(instanceId, recordType, column, updatedColType);
      schema.put(column, updatedColType);
    }
    return schema;
  }

  @GetMapping("/{instanceId}/entities/{version}/{recordType}/{recordId}")
  public ResponseEntity<RecordResponse> getSingleRecord(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("version") String version,
      @PathVariable("recordType") RecordType recordType,
      @PathVariable("recordId") RecordId recordId) {
    validateVersion(version);
    if (!recordDao.workspaceSchemaExists(instanceId)
        || !recordDao.recordTypeExists(instanceId, recordType.getName())) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Instance or table don't exist");
    }
    Record result =
        recordDao.getSingleRecord(
            instanceId,
                recordType,
                recordId,
            recordDao.getReferenceCols(instanceId, recordType.getName()));
    if (result == null) {
      // TODO: standard exception classes
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Record not found");
    }
    RecordResponse response =
        new RecordResponse(
                recordId,
                recordType,
            result.getAttributes(),
            new RecordMetadata("TODO: RECORDMETADATA"));
    return new ResponseEntity<>(response, HttpStatus.OK);
  }

  @PutMapping("/{instanceId}/entities/{version}/{recordType}/{recordId}")
  public ResponseEntity<RecordResponse> putSingleRecord(
      @PathVariable("instanceId") UUID instanceId,
      @PathVariable("version") String version,
      @PathVariable("recordType") RecordType recordType,
      @PathVariable("recordId") RecordId recordId,
      @RequestBody RecordRequest recordRequest) {
    validateVersion(version);
    String recordTypeName = recordType.getName();
    Map<String, Object> attributesInRequest = recordRequest.recordAttributes().getAttributes();
    Map<String, DataTypeMapping> requestSchema = inferer.inferTypes(attributesInRequest);
    if (!recordDao.workspaceSchemaExists(instanceId)) {
      recordDao.createSchema(instanceId);
    }
    try {
      RecordResponse response =
          new RecordResponse(
                  recordId, recordType, recordRequest.recordAttributes(), new RecordMetadata("TODO"));
      if (!recordDao.recordTypeExists(instanceId, recordTypeName)) {
        createRecordTypeAndInsertRecords(instanceId, recordRequest, recordTypeName, requestSchema);
        return new ResponseEntity(response, HttpStatus.CREATED);
      } else {
        Map<String, DataTypeMapping> existingTableSchema =
            recordDao.getExistingTableSchema(instanceId, recordTypeName);
        // null out any attributes that already exist but aren't in the request
        existingTableSchema.keySet().forEach(attr -> attributesInRequest.putIfAbsent(attr, null));
        Record record = new Record(recordId, recordType, recordRequest.recordAttributes());
        List<Record> records = Collections.singletonList(record);
        addOrUpdateColumnIfNeeded(
            instanceId, recordType.getName(), requestSchema, existingTableSchema, records);
        LinkedHashMap<String, DataTypeMapping> combinedSchema =
            new LinkedHashMap<>(existingTableSchema);
        combinedSchema.putAll(requestSchema);
        recordDao.batchUpsert(instanceId, recordTypeName, records, combinedSchema);
        return new ResponseEntity(response, HttpStatus.OK);
      }
    } catch (ResponseStatusException | InvalidRelation e) {
      return new ResponseEntity(e.getMessage(), HttpStatus.BAD_REQUEST);
    }
  }

  @PostMapping("/{instanceId}/{version}/")
  public ResponseEntity<String> createInstance(
      @PathVariable("instanceId") UUID instanceId, @PathVariable("version") String version) {
    validateVersion(version);
    if (recordDao.workspaceSchemaExists(instanceId)) {
      return new ResponseEntity("This schema already exists.", HttpStatus.CONFLICT);
    }
    recordDao.createSchema(instanceId);
    return new ResponseEntity<>(HttpStatus.CREATED);
  }

  private static void validateVersion(String version) {
    Preconditions.checkArgument(version.equals("v0.2"));
  }

  private void createRecordTypeAndInsertRecords(
      UUID instanceId,
      RecordRequest recordRequest,
      String recordTypeName,
      Map<String, DataTypeMapping> requestSchema)
      throws InvalidRelation {
    try {
      Record newRecord =
          new Record(
              recordRequest.recordId(),
              recordRequest.recordType(),
              recordRequest.recordAttributes());
      List<Record> entities = Collections.singletonList(newRecord);
      recordDao.createReccordType(
          instanceId, requestSchema, recordTypeName, RelationUtils.findRelations(entities));
      recordDao.batchUpsert(
          instanceId, recordTypeName, entities, new LinkedHashMap<>(requestSchema));
    } catch (MissingReferencedTableException e) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST,
          "It looks like you're attempting to assign a reference "
              + "to a table that does not exist",
          e);
    }
  }

}
