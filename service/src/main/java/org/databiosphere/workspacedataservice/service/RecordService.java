package org.databiosphere.workspacedataservice.service;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.Relation;
import org.databiosphere.workspacedataservice.service.model.RelationValue;
import org.databiosphere.workspacedataservice.service.model.exception.BatchWriteException;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidRelationException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;

@Service
public class RecordService {

    private final RecordDao recordDao;

    private final DataTypeInferer inferer;

    public RecordService(RecordDao recordDao, DataTypeInferer inferer) {
        this.recordDao = recordDao;
        this.inferer = inferer;
    }

    public void prepareAndUpsert(UUID instanceId, RecordType recordType, List<Record> records,
                                 Map<String, DataTypeMapping> requestSchema) {
        prepareAndUpsert(instanceId, recordType, records, requestSchema, RECORD_ID);
    }

        public void prepareAndUpsert(UUID instanceId, RecordType recordType, List<Record> records,
                                  Map<String, DataTypeMapping> requestSchema, String primaryKey) {
            //Identify relation arrays
            Map<String, DataTypeMapping> relationArrays = requestSchema.entrySet().stream()
                    .filter(entry -> entry.getValue() == DataTypeMapping.ARRAY_OF_RELATION)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<Relation, List<RelationValue>> relationArrayValues = new HashMap<>();
        for (Record rec : records) {
            //Cannot use streaming here - collecting a stream to a map cannot accept null values for the map value
            for (Map.Entry<String, Object> attribute : rec.attributeSet()){
                //TODO A nicer way to do all this?
                if (relationArrays.containsKey(attribute.getKey())){
                    List<String> rels;
                    if (attribute.getValue() instanceof List<?>){
                        rels = (List<String>) attribute.getValue();
                    } else {
                        rels = Arrays.asList(inferer.getArrayOfType(attribute.getValue().toString(), String[].class));
                    }
                    //TODO Create/Use relationutils or datatypeinferer method(s)
                    Relation relDef = new Relation(attribute.getKey(), RelationUtils.getTypeValue(rels.get(0)));
                    List<RelationValue> relList = relationArrayValues.getOrDefault(relDef, new ArrayList<>());
                    for (String r : rels){
                        if (!RelationUtils.getTypeValue(r).equals(relDef.relationRecordType())){
                            throw new InvalidRelationException("It looks like you're attempting to assign a relation "
                                    + "to multiple record types");
                        }
                        relList.add(new RelationValue(rec, new Record(RelationUtils.getRelationValue(r), RelationUtils.getTypeValue(r), new RecordAttributes(Collections.emptyMap()))));
                    }
                    relationArrayValues.put(relDef, relList);
                }
            }
        }
        recordDao.batchUpsert(instanceId, recordType, records, requestSchema, primaryKey);
        for (Map.Entry<Relation, List<RelationValue>> rel : relationArrayValues.entrySet()) {
            recordDao.insertIntoJoin(instanceId, rel.getKey(), recordType, rel.getValue());
        }
    }

    public void batchUpsertWithErrorCapture(UUID instanceId, RecordType recordType, List<Record> records,
                                            Map<String, DataTypeMapping> schema, String primaryKey) {
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

    private List<String> checkEachRow(List<Record> records, Map<String, DataTypeMapping> recordTypeSchema) {
        List<String> result = new ArrayList<>();
        for (Record rcd : records) {
            Map<String, DataTypeMapping> schemaForRecord = inferer.inferTypes(rcd.getAttributes(),
                    InBoundDataSource.JSON);
            if (!schemaForRecord.equals(recordTypeSchema)) {
                MapDifference<String, DataTypeMapping> difference = Maps.difference(schemaForRecord, recordTypeSchema);
                Map<String, MapDifference.ValueDifference<DataTypeMapping>> differenceMap = difference
                        .entriesDiffering();
                result.add(convertSchemaDiffToErrorMessage(differenceMap, rcd));
            }
        }
        return result;
    }

    private String convertSchemaDiffToErrorMessage(
            Map<String, MapDifference.ValueDifference<DataTypeMapping>> differenceMap, Record rcd) {
        return differenceMap.keySet().stream()
                .map(attr -> rcd.getId() + "." + attr + " is a " + differenceMap.get(attr).leftValue()
                        + " in the request but is defined as " + differenceMap.get(attr).rightValue()
                        + " in the record type definition for " + rcd.getRecordType())
                .collect(Collectors.joining("\n"));
    }

    private boolean isDataMismatchException(DataAccessException e) {
        return e.getRootCause()instanceof SQLException sqlException && sqlException.getSQLState().equals("42804");
    }
}
