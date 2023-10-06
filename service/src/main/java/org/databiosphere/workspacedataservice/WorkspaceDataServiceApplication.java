package org.databiosphere.workspacedataservice;

import com.microsoft.applicationinsights.attach.ApplicationInsights;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootApplication(
    scanBasePackages = {
      // this codebase
      "org.databiosphere.workspacedataservice",
      // terra-common-lib transaction management and DB retry configuration
      "bio.terra.common.retry.transaction"
    })
@EnableRetry
@EnableTransactionManagement
@EnableCaching
public class WorkspaceDataServiceApplication {

  public static void main(String[] args) {
    ApplicationInsights.attach();
    SpringApplication.run(WorkspaceDataServiceApplication.class, args);
  }

  @Bean
  public OpenTelemetry openTelemetry() {
    return AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk();
  }
}
