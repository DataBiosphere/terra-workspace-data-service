package org.databiosphere.workspacedataservice.recordsink;

import java.util.List;
import java.util.Map;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RecordService;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.DataImportException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
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
  private final CollectionId collectionId;

  WdsRecordSink(
      RecordService recordService,
      RecordDao recordDao,
      DataTypeInferer inferer,
      CollectionId collectionId) {
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
    if (!recordDao.recordTypeExists(collectionId.id(), recordType)) {
      recordDao.createRecordType(
          collectionId.id(),
          schema,
          recordType,
          inferer.findRelations(records, schema),
          recordTypePrimaryKey);
    } else {
      return recordService.addOrUpdateColumnIfNeeded(
          collectionId.id(),
          recordType,
          schema,
          recordDao.getExistingTableSchemaLessPrimaryKey(collectionId.id(), recordType),
          records);
    }
    return schema;
  }

  @Override
  public void upsertBatch(
      RecordType recordType,
      Map<String, DataTypeMapping> schema,
      List<Record> records,
      String primaryKey) {
    recordService.batchUpsert(collectionId.id(), recordType, records, schema, primaryKey);
  }

  @Override
  public void deleteBatch(RecordType recordType, List<Record> records) {
    recordDao.batchDelete(collectionId.id(), recordType, records);
  }

  @Override
  public void success() throws DataImportException {
    // noop
  }
}
