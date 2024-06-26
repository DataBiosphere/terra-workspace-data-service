package org.databiosphere.workspacedataservice.controller;

import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * We sample using the first batch, so in order to test the data mismatch error condition we have to
 * set a small batch size for the test.
 */
@TestConfiguration
class SmallBatchWriteTestConfig {

  @Bean
  public BatchWriteService batchWriteService(DataTypeInferer dataTypeInferer) {
    return new BatchWriteService(/* batchSize= */ 1, dataTypeInferer);
  }
}
