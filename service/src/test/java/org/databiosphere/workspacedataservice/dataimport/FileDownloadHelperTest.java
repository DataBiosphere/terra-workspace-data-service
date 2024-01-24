package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;

@SpringBootTest
public class FileDownloadHelperTest {

  @Value("classpath:parquet/empty.parquet")
  Resource emptyParquet;

  @Test
  void downloadEmptyFile() throws IOException {
    FileDownloadHelper helper = new FileDownloadHelper("test");
    assertDoesNotThrow(() -> helper.downloadFileFromURL("empty_table", emptyParquet.getURL()));
    assert helper.getFileMap().isEmpty();
  }
}
