package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.service.BatchWriteService.RecordSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BatchWriteServiceConfig {
  private static final Logger logger = LoggerFactory.getLogger(BatchWriteServiceConfig.class);

  private final TwdsProperties twdsProperties;
  private RecordService recordService;

  public BatchWriteServiceConfig(TwdsProperties twdsProperties) {
    this.twdsProperties = twdsProperties;
  }

  @Autowired(required = false) // RecordService only required for RecordSinkMode.WDS
  public void setRecordService(RecordService recordService) {
    this.recordService = recordService;
  }

  @Bean
  RecordSink recordSink() {
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
        // TODO(AJ-1589): make prefix assignment dynamic
        return RawlsRecordSink.withPrefix("pfb");
      }
      default -> throw new RuntimeException(
          "Unknown RecordSinkMode: %s"
              .formatted(twdsProperties.getDataImport().getBatchWriteRecordSink()));
    }
  }
}
