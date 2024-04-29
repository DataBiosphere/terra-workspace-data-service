package org.databiosphere.workspacedataservice.logging;

import static org.zalando.logbook.core.Conditions.exclude;
import static org.zalando.logbook.core.Conditions.requestTo;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.zalando.logbook.Logbook;

@Configuration
public class LogbookConfiguration {

  @Bean
  public Logbook logbook() {
    Logbook logbook = Logbook.builder().condition(exclude(requestTo("/status/**"))).build();
    return logbook;
  }
}
