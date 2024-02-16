package org.databiosphere.workspacedataservice.recordsink;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import java.util.function.Consumer;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.recordsink.RawlsRecordSink.RawlsJsonConsumer;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RecordService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RecordSinkFactory {
  private static final Logger logger = LoggerFactory.getLogger(RecordSinkFactory.class);

  private final TwdsProperties twdsProperties;
  private final ObjectMapper mapper;
  private RecordService recordService;
  private RecordDao recordDao;
  private DataTypeInferer dataTypeInferer;

  private Consumer<String> jsonConsumer;

  public RecordSinkFactory(TwdsProperties twdsProperties, ObjectMapper mapper) {
    this.twdsProperties = twdsProperties;
    this.mapper = mapper;
  }

  @Autowired(required = false) // RecordService only required for RecordSinkMode.WDS
  public void setRecordService(RecordService recordService) {
    this.recordService = recordService;
  }

  @Autowired(required = false) // RecordService only required for RecordSinkMode.WDS
  public void setRecordDao(RecordDao recordDao) {
    this.recordDao = recordDao;
  }

  @Autowired(required = false) // RecordService only required for RecordSinkMode.WDS
  public void setDataTypeInferer(DataTypeInferer dataTypeInferer) {
    this.dataTypeInferer = dataTypeInferer;
  }

  @Autowired(required = false) // jsonConsumer only required for RecordSinkMode.RAWLS
  public void setJsonConsumer(@RawlsJsonConsumer Consumer<String> jsonConsumer) {
    this.jsonConsumer = jsonConsumer;
  }

  // TODO(AJ-1589): make prefix assignment dynamic
  public RecordSink buildRecordSink(UUID collectionId, String prefix) {
    if (twdsProperties.getDataImport() == null) {
      logger
          .atWarn()
          .log(
              "twds.data-import properties are not defined. "
                  + "Start this deployment with active Spring profile of "
                  + "either 'data-plane' or 'control-plane'. "
                  + "Defaulting to batch-write-record-sink=wds");
      return wdsRecordSink(collectionId);
    }

    switch (twdsProperties.getDataImport().getBatchWriteRecordSink()) {
      case WDS -> {
        return wdsRecordSink(collectionId);
      }
      case RAWLS -> {
        return rawlsRecordSink(prefix);
      }
      default -> throw new RuntimeException(
          "Unknown RecordSinkMode: %s"
              .formatted(twdsProperties.getDataImport().getBatchWriteRecordSink()));
    }
  }

  private RecordSink rawlsRecordSink(String prefix) {
    return new RawlsRecordSink(prefix, mapper, jsonConsumer);
  }

  private WdsRecordSink wdsRecordSink(UUID collectionId) {
    return new WdsRecordSink(recordService, recordDao, dataTypeInferer, collectionId);
  }
}
