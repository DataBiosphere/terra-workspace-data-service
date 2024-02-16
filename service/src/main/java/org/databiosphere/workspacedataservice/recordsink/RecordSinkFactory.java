package org.databiosphere.workspacedataservice.recordsink;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Consumer;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.recordsink.RawlsRecordSink.RawlsJsonConsumer;
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
  private Consumer<String> jsonConsumer;

  public RecordSinkFactory(TwdsProperties twdsProperties, ObjectMapper mapper) {
    this.twdsProperties = twdsProperties;
    this.mapper = mapper;
  }

  @Autowired(required = false) // RecordService only required for RecordSinkMode.WDS
  public void setRecordService(RecordService recordService) {
    this.recordService = recordService;
  }

  @Autowired(required = false) // jsonConsumer only required for RecordSinkMode.RAWLS
  public void setJsonConsumer(@RawlsJsonConsumer Consumer<String> jsonConsumer) {
    this.jsonConsumer = jsonConsumer;
  }

  // TODO(AJ-1589): make prefix assignment dynamic
  public RecordSink buildRecordSink(String prefix) {
    if (twdsProperties.getDataImport() == null) {
      logger
          .atWarn()
          .log(
              "twds.data-import properties are not defined. "
                  + "Start this deployment with active Spring profile of "
                  + "either 'data-plane' or 'control-plane'. "
                  + "Defaulting to batch-write-record-sink=wds");
      return recordService;
    }

    switch (twdsProperties.getDataImport().getBatchWriteRecordSink()) {
      case WDS -> {
        return recordService;
      }
      case RAWLS -> {
        return new RawlsRecordSink(prefix, mapper, jsonConsumer);
      }
      default -> throw new RuntimeException(
          "Unknown RecordSinkMode: %s"
              .formatted(twdsProperties.getDataImport().getBatchWriteRecordSink()));
    }
  }
}
