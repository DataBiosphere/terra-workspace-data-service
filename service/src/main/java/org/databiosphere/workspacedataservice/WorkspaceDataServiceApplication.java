package org.databiosphere.workspacedataservice;

import com.microsoft.applicationinsights.attach.ApplicationInsights;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableScheduling;
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
@EnableScheduling
public class WorkspaceDataServiceApplication {

  public static void main(String[] args) {
    startApplicationInsights();
    SpringApplication.run(WorkspaceDataServiceApplication.class, args);
  }

  private static void startApplicationInsights() {
    try {
      if (StringUtils.isNotBlank(System.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING"))) {
        ApplicationInsights.attach();
      }
    } catch (Exception e) {
      Logger logger = LoggerFactory.getLogger(WorkspaceDataServiceApplication.class);
      logger.warn("Error while attempting to start Application Insights: {}", e.getMessage());
    }
  }
}
