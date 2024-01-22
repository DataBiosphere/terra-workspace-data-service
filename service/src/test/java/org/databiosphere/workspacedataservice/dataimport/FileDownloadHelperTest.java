package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.support.RetryTemplate;

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

  @Test
  void testRetry() throws Exception {
    FileDownloadHelper.DownloadHelper mockDownloadHelper =
        Mockito.mock(FileDownloadHelper.DownloadHelper.class);
    doThrow(new IOException("Simulated connection error"))
        .doCallRealMethod() // Succeed on the second attempt
        .when(mockDownloadHelper)
        .copyURLToFile(any(URL.class), any(File.class));

    // Create a RetryTemplate to set off Spring's retryable
    RetryTemplate retryTemplate = new RetryTemplate();
    retryTemplate.setBackOffPolicy(new FixedBackOffPolicy());

    FileDownloadHelper helper = new FileDownloadHelper("test", mockDownloadHelper);

    // A single connectivity error should not throw
    assertDoesNotThrow(
        () ->
            retryTemplate.execute(
                context -> {
                  helper.downloadFileFromURL("table", allDataTypesParquet.getURL());
                  return null;
                }));

    // Make sure there actually was a connectivity problem
    verify(mockDownloadHelper, times(2)).copyURLToFile(any(URL.class), any(File.class));

    // File should successfully download on second attempt
    assert helper.getFileMap().containsKey("table");
    assert helper.getFileMap().get("table").size() == 1;
  }
}
