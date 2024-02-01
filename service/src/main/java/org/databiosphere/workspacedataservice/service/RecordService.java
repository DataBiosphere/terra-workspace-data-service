package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RESERVED_NAME_PREFIX;

import bio.terra.common.db.WriteTransaction;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.service.model.RelationValue;
import org.databiosphere.workspacedataservice.service.model.ReservedNames;
import org.databiosphere.workspacedataservice.service.model.exception.BatchWriteException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidNameException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.NewPrimaryKeyException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class RecordService {
  // strings used for metrics
  public static final String COUNTER_COL_CHANGE = "column.change.datatype";
  public static final String TAG_RECORD_TYPE = "RecordType";
  public static final String TAG_INSTANCE = "Instance";
  public static final String TAG_ATTRIBUTE_NAME = "AttributeName";
  public static final String TAG_OLD_DATATYPE = "OldDataType";
  public static final String TAG_NEW_DATATYPE = "NewDataType";

  private final RecordDao recordDao;

  private final DataTypeInferer inferer;

  private final MeterRegistry meterRegistry;

  public RecordService(RecordDao recordDao, DataTypeInferer inferer, MeterRegistry meterRegistry) {
    this.recordDao = recordDao;
    this.inferer = inferer;
    this.meterRegistry = meterRegistry;
  }

  public void prepareAndUpsert(
      UUID instanceId,
      RecordType recordType,
      List<Record> records,
      Map<String, DataTypeMapping> requestSchema,
      String primaryKey) {
    // Identify relation arrays
    Map<String, DataTypeMapping> relationArrays =
        requestSchema.entrySet().stream()
            .filter(entry -> entry.getValue() == DataTypeMapping.ARRAY_OF_RELATION)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Map<Relation, List<RelationValue>> relationArrayValues =
        getAllRelationArrayValues(records, relationArrays);
    recordDao.batchUpsert(instanceId, recordType, records, requestSchema, primaryKey);
    for (Map.Entry<Relation, List<RelationValue>> rel : relationArrayValues.entrySet()) {
      // remove existing values from join table and replace with new ones
      recordDao.removeFromJoin(
          instanceId,
          rel.getKey(),
          recordType,
          rel.getValue().stream().map(relVal -> relVal.fromRecord().getId()).toList());
      recordDao.insertIntoJoin(instanceId, rel.getKey(), recordType, rel.getValue());
    }
  }

  private Map<Relation, List<RelationValue>> getAllRelationArrayValues(
      List<Record> records, Map<String, DataTypeMapping> relationArrays) {
    Map<Relation, List<RelationValue>> relationArrayValues = new HashMap<>();
    for (Record rec : records) {
      // find relation array attributes for this record
      List<Map.Entry<String, Object>> arrayAttributesForThisRecord =
          rec.attributeSet().stream()
              .filter(
                  entry -> entry.getValue() != null && relationArrays.containsKey(entry.getKey()))
              .toList();
      for (Map.Entry<String, Object> attribute : arrayAttributesForThisRecord) {
        // How to read relation list depends on its source, which we don't know here so we have to
        // check
        List<String> rels;
        if (attribute.getValue() instanceof List<?>) {
          rels = (List<String>) attribute.getValue();
        } else {
          rels =
              Arrays.asList(
                  inferer.getArrayOfType(attribute.getValue().toString(), String[].class));
        }
        Relation relDef = new Relation(attribute.getKey(), RelationUtils.getTypeValueForList(rels));
        List<RelationValue> relList = relationArrayValues.getOrDefault(relDef, new ArrayList<>());
        relList.addAll(rels.stream().map(r -> createRelationValue(rec, r)).toList());
        relationArrayValues.put(relDef, relList);
      }
    }

    return relationArrayValues;
  }

  private RelationValue createRelationValue(Record fromRecord, String toString) {
    return new RelationValue(
        fromRecord,
        new Record(
            RelationUtils.getRelationValue(toString),
            RelationUtils.getTypeValue(toString),
            new RecordAttributes(Collections.emptyMap())));
  }

  public void batchUpsertWithErrorCapture(
      UUID instanceId,
      RecordType recordType,
      List<Record> records,
      Map<String, DataTypeMapping> schema,
      String primaryKey) {
    try {
      prepareAndUpsert(instanceId, recordType, records, schema, primaryKey);
    } catch (DataAccessException e) {
      if (isDataMismatchException(e)) {
        Map<String, DataTypeMapping> recordTypeSchemaWithoutId = new HashMap<>(schema);
        recordTypeSchemaWithoutId.remove(primaryKey);
        List<String> rowErrors = checkEachRow(records, recordTypeSchemaWithoutId);
        if (!rowErrors.isEmpty()) {
          throw new BatchWriteException(rowErrors);
        }
      }
      throw e;
    }
  }

  private List<String> checkEachRow(
      List<Record> records, Map<String, DataTypeMapping> recordTypeSchema) {
    List<String> result = new ArrayList<>();
    for (Record rcd : records) {
      Map<String, DataTypeMapping> schemaForRecord = inferer.inferTypes(rcd.getAttributes());
      if (!schemaForRecord.equals(recordTypeSchema)) {
        MapDifference<String, DataTypeMapping> difference =
            Maps.difference(schemaForRecord, recordTypeSchema);
        Map<String, MapDifference.ValueDifference<DataTypeMapping>> differenceMap =
            difference.entriesDiffering();
        result.add(convertSchemaDiffToErrorMessage(differenceMap, rcd));
      }
    }
    return result;
  }

  private String convertSchemaDiffToErrorMessage(
      Map<String, MapDifference.ValueDifference<DataTypeMapping>> differenceMap, Record rcd) {
    return differenceMap.keySet().stream()
        .map(
            attr ->
                rcd.getId()
                    + "."
                    + attr
                    + " is a "
                    + differenceMap.get(attr).leftValue()
                    + " in the request but is defined as "
                    + differenceMap.get(attr).rightValue()
                    + " in the record type definition for "
                    + rcd.getRecordType())
        .collect(Collectors.joining("\n"));
  }

  private boolean isDataMismatchException(DataAccessException e) {
    return e.getRootCause() instanceof SQLException sqlException
        && sqlException.getSQLState().equals("42804");
  }

  public Map<String, DataTypeMapping> addOrUpdateColumnIfNeeded(
      UUID instanceId,
      RecordType recordType,
      Map<String, DataTypeMapping> schema,
      Map<String, DataTypeMapping> existingTableSchema,
      List<Record> records) {
    MapDifference<String, DataTypeMapping> difference =
        Maps.difference(existingTableSchema, schema);
    Map<String, DataTypeMapping> colsToAdd = difference.entriesOnlyOnRight();
    colsToAdd.keySet().stream()
        .filter(s -> s.startsWith(RESERVED_NAME_PREFIX))
        .findAny()
        .ifPresent(
            s -> {
              throw new InvalidNameException(InvalidNameException.NameType.ATTRIBUTE);
            });
    validateRelationsAndAddColumns(
        instanceId, recordType, schema, records, colsToAdd, existingTableSchema);
    Map<String, MapDifference.ValueDifference<DataTypeMapping>> differenceMap =
        difference.entriesDiffering();
    for (Map.Entry<String, MapDifference.ValueDifference<DataTypeMapping>> entry :
        differenceMap.entrySet()) {
      String column = entry.getKey();
      MapDifference.ValueDifference<DataTypeMapping> valueDifference = entry.getValue();
      // Don't allow updating relation columns
      if (valueDifference.leftValue() == DataTypeMapping.ARRAY_OF_RELATION
          || valueDifference.leftValue() == DataTypeMapping.RELATION) {
        throw new InvalidRelationException(
            "Unable to update a relation or array of relation attribute type");
      }
      DataTypeMapping updatedColType =
          inferer.selectBestType(valueDifference.leftValue(), valueDifference.rightValue());
      recordDao.changeColumn(instanceId, recordType, column, updatedColType);
      schema.put(column, updatedColType);

      // update a metrics counter with this schema change
      Counter counter =
          Counter.builder(COUNTER_COL_CHANGE)
              .tag(TAG_RECORD_TYPE, recordType.getName())
              .tag(TAG_ATTRIBUTE_NAME, column)
              .tag(TAG_INSTANCE, instanceId.toString())
              .tag(TAG_OLD_DATATYPE, valueDifference.leftValue().toString())
              .tag(TAG_NEW_DATATYPE, updatedColType.toString())
              .description("Column schema changes")
              .register(meterRegistry);
      counter.increment();
    }
    return schema;
  }

  public void validateRelations(
      Set<Relation> existingRelations,
      Set<Relation> newRelations,
      Map<String, DataTypeMapping> existingSchema) {
    Set<String> existingRelationCols =
        existingRelations.stream().map(Relation::relationColName).collect(Collectors.toSet());
    // look for case where requested relation column already exists as a
    // non-relational column
    for (Relation relation : newRelations) {
      String col = relation.relationColName();
      if (!existingRelationCols.contains(col) && existingSchema.containsKey(col)) {
        throw new InvalidRelationException(
            "It looks like you're attempting to assign a relation "
                + "to an existing attribute that was not configured for relations");
      }
    }
  }

  public void validateRelationsAndAddColumns(
      UUID instanceId,
      RecordType recordType,
      Map<String, DataTypeMapping> requestSchema,
      List<Record> records,
      Map<String, DataTypeMapping> colsToAdd,
      Map<String, DataTypeMapping> existingSchema) {
    RelationCollection relations = inferer.findRelations(records, requestSchema);
    RelationCollection existingRelations =
        new RelationCollection(
            Set.copyOf(recordDao.getRelationCols(instanceId, recordType)),
            Set.copyOf(recordDao.getRelationArrayCols(instanceId, recordType)));
    // look for case where requested relation column already exists as a
    // non-relational column
    validateRelations(existingRelations.relations(), relations.relations(), existingSchema);
    // same for relation-array columns
    validateRelations(
        existingRelations.relationArrays(), relations.relationArrays(), existingSchema);
    relations.relations().addAll(existingRelations.relations());
    relations.relationArrays().addAll(existingRelations.relationArrays());
    Map<String, List<Relation>> allRefCols =
        relations.relations().stream().collect(Collectors.groupingBy(Relation::relationColName));
    if (allRefCols.values().stream().anyMatch(l -> l.size() > 1)) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "Relation attribute can only be assigned to one record type");
    }
    Map<String, List<Relation>> allRefArrCols =
        relations.relationArrays().stream()
            .collect(Collectors.groupingBy(Relation::relationColName));
    if (allRefArrCols.values().stream().anyMatch(l -> l.size() > 1)) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST,
          "Relation array attribute can only be assigned to one record type");
    }
    for (Map.Entry<String, DataTypeMapping> entry : colsToAdd.entrySet()) {
      RecordType referencedRecordType = null;
      String col = entry.getKey();
      DataTypeMapping dataType = entry.getValue();
      if (allRefCols.containsKey(col)) {
        referencedRecordType = allRefCols.get(col).get(0).relationRecordType();
      }
      recordDao.addColumn(instanceId, recordType, col, dataType, referencedRecordType);
      if (allRefArrCols.containsKey(col)) {
        referencedRecordType = allRefArrCols.get(col).get(0).relationRecordType();
        recordDao.createRelationJoinTable(instanceId, col, recordType, referencedRecordType);
      }
      requestSchema.put(col, dataType);
    }
  }

  @WriteTransaction
  public RecordResponse updateSingleRecord(
      UUID instanceId, RecordType recordType, String recordId, RecordRequest recordRequest) {
    Record singleRecord =
        recordDao
            .getSingleRecord(instanceId, recordType, recordId)
            .orElseThrow(() -> new MissingObjectException("Record"));
    RecordAttributes incomingAtts = recordRequest.recordAttributes();
    RecordAttributes allAttrs = singleRecord.putAllAttributes(incomingAtts).getAttributes();
    Map<String, DataTypeMapping> typeMapping = inferer.inferTypes(incomingAtts);
    Map<String, DataTypeMapping> existingTableSchema =
        recordDao.getExistingTableSchemaLessPrimaryKey(instanceId, recordType);
    singleRecord.setAttributes(allAttrs);
    List<Record> records = Collections.singletonList(singleRecord);
    Map<String, DataTypeMapping> updatedSchema =
        addOrUpdateColumnIfNeeded(
            instanceId, recordType, typeMapping, existingTableSchema, records);
    prepareAndUpsert(
        instanceId,
        recordType,
        records,
        updatedSchema,
        recordDao.getPrimaryKeyColumn(recordType, instanceId));
    return new RecordResponse(recordId, recordType, singleRecord.getAttributes());
  }

  @WriteTransaction
  public ResponseEntity<RecordResponse> upsertSingleRecord(
      UUID instanceId,
      RecordType recordType,
      String recordId,
      Optional<String> primaryKey,
      RecordRequest recordRequest) {
    RecordAttributes attributesInRequest = recordRequest.recordAttributes();
    Map<String, DataTypeMapping> requestSchema = inferer.inferTypes(attributesInRequest);
    HttpStatus status = HttpStatus.CREATED;
    if (!recordDao.recordTypeExists(instanceId, recordType)) {
      RecordResponse response =
          new RecordResponse(recordId, recordType, recordRequest.recordAttributes());
      Record newRecord = new Record(recordId, recordType, recordRequest);
      createRecordTypeAndInsertRecords(
          instanceId, newRecord, recordType, requestSchema, primaryKey);
      return new ResponseEntity<>(response, status);
    } else {
      validatePrimaryKey(instanceId, recordType, primaryKey);
      Map<String, DataTypeMapping> existingTableSchema =
          recordDao.getExistingTableSchemaLessPrimaryKey(instanceId, recordType);
      // null out any attributes that already exist but aren't in the request
      existingTableSchema
          .keySet()
          .forEach(attr -> attributesInRequest.putAttributeIfAbsent(attr, null));
      if (recordDao.recordExists(instanceId, recordType, recordId)) {
        status = HttpStatus.OK;
      }
      Record newRecord = new Record(recordId, recordType, recordRequest.recordAttributes());
      List<Record> records = Collections.singletonList(newRecord);
      addOrUpdateColumnIfNeeded(
          instanceId, recordType, requestSchema, existingTableSchema, records);
      Map<String, DataTypeMapping> combinedSchema = new HashMap<>(existingTableSchema);
      combinedSchema.putAll(requestSchema);
      prepareAndUpsert(
          instanceId,
          recordType,
          records,
          combinedSchema,
          primaryKey.orElseGet(() -> recordDao.getPrimaryKeyColumn(recordType, instanceId)));
      RecordResponse response = new RecordResponse(recordId, recordType, attributesInRequest);
      return new ResponseEntity<>(response, status);
    }
  }

  private void createRecordTypeAndInsertRecords(
      UUID instanceId,
      Record newRecord,
      RecordType recordType,
      Map<String, DataTypeMapping> requestSchema,
      Optional<String> primaryKey) {
    List<Record> records = Collections.singletonList(newRecord);
    recordDao.createRecordType(
        instanceId,
        requestSchema,
        recordType,
        inferer.findRelations(records, requestSchema),
        primaryKey.orElse(ReservedNames.RECORD_ID));
    prepareAndUpsert(
        instanceId, recordType, records, requestSchema, primaryKey.orElse(ReservedNames.RECORD_ID));
  }

  public String validatePrimaryKey(
      UUID instanceId, RecordType recordType, Optional<String> primaryKey) {
    String existingKey = recordDao.getPrimaryKeyColumn(recordType, instanceId);
    if (primaryKey.isPresent() && !primaryKey.get().equals(existingKey)) {
      throw new NewPrimaryKeyException(primaryKey.get(), recordType);
    }
    return existingKey;
  }

  @WriteTransaction
  public boolean deleteSingleRecord(UUID instanceId, RecordType recordType, String recordId) {
    return recordDao.deleteSingleRecord(instanceId, recordType, recordId);
  }

  @WriteTransaction
  public void deleteRecordType(UUID instanceId, RecordType recordType) {
    recordDao.deleteRecordType(instanceId, recordType);
  }

  @WriteTransaction
  public void renameAttribute(
      UUID instanceId, RecordType recordType, String attribute, String newAttributeName) {
    recordDao.renameAttribute(instanceId, recordType, attribute, newAttributeName);
  }

  @WriteTransaction
  public void updateAttributeDataType(
      UUID instanceId, RecordType recordType, String attribute, DataTypeMapping newDataType) {
    recordDao.updateAttributeDataType(instanceId, recordType, attribute, newDataType);
  }

  @WriteTransaction
  public void deleteAttribute(UUID instanceId, RecordType recordType, String attribute) {
    recordDao.deleteAttribute(instanceId, recordType, attribute);
  }
}
