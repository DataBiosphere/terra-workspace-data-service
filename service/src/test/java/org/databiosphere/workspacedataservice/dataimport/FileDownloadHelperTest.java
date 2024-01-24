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

  @Value("classpath:parquet/v2f/all_data_types.parquet")
  Resource allDataTypesParquet;

  @Test
  void downloadEmptyFile() throws IOException {
    FileDownloadHelper helper = new FileDownloadHelper("test");
    assertDoesNotThrow(() -> helper.downloadFileFromURL("empty_table", emptyParquet.getURL()));
    assert helper.getFileMap().isEmpty();
  }
}
