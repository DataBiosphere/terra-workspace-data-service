package org.databiosphere.workspacedataservice.logging;

import static org.zalando.logbook.core.Conditions.exclude;
import static org.zalando.logbook.core.Conditions.requestTo;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.zalando.logbook.Logbook;
import org.zalando.logbook.core.CommonsLogFormatSink;
import org.zalando.logbook.core.DefaultHttpLogWriter;

@Configuration
public class LogbookConfiguration {

  @Bean
  public Logbook logbook() {
    return Logbook.builder()
        .condition(
            exclude(
                requestTo("/status/liveness"),
                requestTo("/status/readiness"),
                requestTo("/prometheus")))
        .sink(new CommonsLogFormatSink(new DefaultHttpLogWriter()))
        .build();
  }
}
