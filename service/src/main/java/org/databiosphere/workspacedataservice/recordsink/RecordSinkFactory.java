package org.databiosphere.workspacedataservice.recordsink;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import java.util.function.Consumer;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.recordsink.RawlsRecordSink.RawlsJsonConsumer;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RecordService;
import org.databiosphere.workspacedataservice.storage.GcsStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RecordSinkFactory {
  private static final Logger logger = LoggerFactory.getLogger(RecordSinkFactory.class);

  private final DataImportProperties dataImportProperties;
  private final ObjectMapper mapper;
  private RecordService recordService;
  private RecordDao recordDao;
  private DataTypeInferer dataTypeInferer;
  private GcsStorage storage;
  private Consumer<String> jsonConsumer;

  public RecordSinkFactory(
      DataImportProperties dataImportProperties, ObjectMapper mapper, GcsStorage storage) {
    this.dataImportProperties = dataImportProperties;
    this.mapper = mapper;
    this.storage = storage;
  }

  @Autowired(required = false) // RecordService only required for RecordSinkMode.WDS
  public void setRecordService(RecordService recordService) {
    this.recordService = recordService;
  }

  @Autowired(required = false) // RecordDao only required for RecordSinkMode.WDS
  public void setRecordDao(RecordDao recordDao) {
    this.recordDao = recordDao;
  }

  @Autowired(required = false) // DataTypeInferer only required for RecordSinkMode.WDS
  public void setDataTypeInferer(DataTypeInferer dataTypeInferer) {
    this.dataTypeInferer = dataTypeInferer;
  }

  @Autowired(required = false) // jsonConsumer only required for RecordSinkMode.RAWLS
  public void setJsonConsumer(@RawlsJsonConsumer Consumer<String> jsonConsumer) {
    this.jsonConsumer = jsonConsumer;
  }

  // TODO(AJ-1589): make prefix assignment dynamic. However, of note: the prefix is currently
  //   ignored for RecordSinkMode.WDS.  In this case, it might be worth adding support for omitting
  //   the prefix as part of supporting the prefix assignment.
  public RecordSink buildRecordSink(UUID collectionId, String prefix) {
    switch (dataImportProperties.getBatchWriteRecordSink()) {
      case WDS -> {
        return wdsRecordSink(collectionId);
      }
      case RAWLS -> {
        return rawlsRecordSink(prefix);
      }
      default -> throw new RuntimeException(
          "Unknown RecordSinkMode: %s".formatted(dataImportProperties.getBatchWriteRecordSink()));
    }
  }

  private RecordSink rawlsRecordSink(String prefix) {
    return new RawlsRecordSink(prefix, mapper, storage, jsonConsumer);
  }

  private WdsRecordSink wdsRecordSink(UUID collectionId) {
    return new WdsRecordSink(recordService, recordDao, dataTypeInferer, collectionId);
  }
}
