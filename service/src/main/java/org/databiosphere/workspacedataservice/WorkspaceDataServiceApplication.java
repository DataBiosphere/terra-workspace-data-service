package org.databiosphere.workspacedataservice;

import com.microsoft.applicationinsights.attach.ApplicationInsights;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
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
    // TODO AJ-1621: if APPLICATIONINSIGHTS_CONNECTION_STRING is empty, don't attempt to attach
    ApplicationInsights.attach();
    SpringApplication.run(WorkspaceDataServiceApplication.class, args);
  }
}
