package org.databiosphere.workspacedataservice.service;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BatchWriteServiceConfig {
  @Bean
  BatchWriteService.RecordSink recordSink(RecordService recordService) {
    return recordService;
  }
}
