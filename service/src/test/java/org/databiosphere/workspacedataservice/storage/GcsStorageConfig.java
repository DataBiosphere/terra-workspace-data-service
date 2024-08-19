package org.databiosphere.workspacedataservice.storage;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.unit.DataSize;

@Configuration
@SpringBootTest
public class GcsStorageConfig {
  private static final String BUCKET_NAME = "test-bucket";

  @Bean
  public GcsStorage mockGcsStorage() {
    Storage mockStorage = LocalStorageHelper.getOptions().getService();
    // this is a hack to work around FakeStorageRpc, which fails when chunk size exceeded due
    // to not being able to handle Content-Range header with an unspecified size
    DataSize batchWriteChunkSize = DataSize.ofMegabytes(64);
    return new GcsStorageImpl(mockStorage, BUCKET_NAME, batchWriteChunkSize);
  }
}
