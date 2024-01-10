package org.databiosphere.workspacedataservice.dataimport.pfb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PfbConfig {
  @Bean
  public PfbRecordConverter getPfbRecordConverter(ObjectMapper objectMapper) {
    return new PfbRecordConverter(objectMapper);
  }
}
