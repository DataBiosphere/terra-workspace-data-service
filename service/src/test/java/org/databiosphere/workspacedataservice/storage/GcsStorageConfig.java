package org.databiosphere.workspacedataservice.storage;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@SpringBootTest
public class GcsStorageConfig {
  private String bucketName = "test-bucket";

  private String projectId = "test-projectId";

  @Bean
  public GcsStorage mockGcsStorage() {
    Storage mockStorage = LocalStorageHelper.getOptions().getService();
    return new GcsStorage(mockStorage, bucketName, projectId);
  }
}
