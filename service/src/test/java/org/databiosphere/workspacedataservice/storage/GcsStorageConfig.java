package org.databiosphere.workspacedataservice.storage;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@SpringBootTest
public class GcsStorageConfig {
  private static final String BUCKET_NAME = "test-bucket";

  @Bean
  public GcsStorage mockGcsStorage() {
    Storage mockStorage = LocalStorageHelper.getOptions().getService();
    return new GcsStorage(mockStorage, BUCKET_NAME);
  }
}
