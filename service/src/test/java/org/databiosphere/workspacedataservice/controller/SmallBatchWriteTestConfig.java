package org.databiosphere.workspacedataservice.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.dataimport.pfb.PfbRecordConverter;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.service.RecordService;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * We sample using the first batch, so in order to test the data mismatch error condition we have to
 * set a small batch size for the test.
 */
@TestConfiguration
public class SmallBatchWriteTestConfig {

  @Bean
  public BatchWriteService batchWriteService(
      RecordDao recordDao,
      DataTypeInferer inf,
      ObjectMapper objectMapper,
      ObjectReader tsvReader,
      RecordService recordService,
      PfbRecordConverter pfbRecordConverter) {
    return new BatchWriteService(
        recordDao, 1, inf, objectMapper, tsvReader, recordService, pfbRecordConverter);
  }
}
