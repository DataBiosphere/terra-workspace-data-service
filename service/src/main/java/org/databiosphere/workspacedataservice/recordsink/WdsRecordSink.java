package org.databiosphere.workspacedataservice.recordsink;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RecordService;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.BatchWriteException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

/**
 * {@link RecordSink} implementation that records batches of writes/deletes to the Workspace Data
 * Service storage, adjusting the schema as needed.
 */
public class WdsRecordSink implements RecordSink {

  private final RecordService recordService;
  private final RecordDao recordDao;
  private final DataTypeInferer inferer;
  private final UUID collectionId;

  public String getBlobName() {
    return "";
  }

  WdsRecordSink(
      RecordService recordService,
      RecordDao recordDao,
      DataTypeInferer inferer,
      UUID collectionId) {
    this.recordService = recordService;
    this.recordDao = recordDao;
    this.inferer = inferer;
    this.collectionId = collectionId;
  }

  @Override
  public Map<String, DataTypeMapping> createOrModifyRecordType(
      RecordType recordType,
      Map<String, DataTypeMapping> schema,
      List<Record> records,
      String recordTypePrimaryKey) {
    if (!recordDao.recordTypeExists(collectionId, recordType)) {
      recordDao.createRecordType(
          collectionId,
          schema,
          recordType,
          inferer.findRelations(records, schema),
          recordTypePrimaryKey);
    } else {
      return recordService.addOrUpdateColumnIfNeeded(
          collectionId,
          recordType,
          schema,
          recordDao.getExistingTableSchemaLessPrimaryKey(collectionId, recordType),
          records);
    }
    return schema;
  }

  @Override
  public void upsertBatch(
      RecordType recordType,
      Map<String, DataTypeMapping> schema,
      List<Record> records,
      String primaryKey)
      throws BatchWriteException {
    recordService.batchUpsert(collectionId, recordType, records, schema, primaryKey);
  }

  @Override
  public void deleteBatch(RecordType recordType, List<Record> records) throws BatchWriteException {
    recordDao.batchDelete(collectionId, recordType, records);
  }
}
