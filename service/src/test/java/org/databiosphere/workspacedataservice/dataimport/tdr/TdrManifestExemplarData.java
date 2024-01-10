package org.databiosphere.workspacedataservice.dataimport.tdr;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

/**
 * (partial) Java representations of the test manifests located in
 * service/src/test/resources/tdrmanifest
 */
public class TdrManifestExemplarData {

  /** the "azure_small.json" file */
  static class AzureSmall {

    static List<URL> projectParquetUrls;
    static List<URL> edgesParquetUrls;
    static List<URL> testResultParquetUrls;

    static {
      try {
        projectParquetUrls =
            List.of(
                new URL(
                    "https://mysnapshotsa.blob.core.windows.net/metadata/parquet/9516afec-583f-11ec-bf63-0242ac130002/project.parquet/F0EE365B-314D-4E19-A177-E8F63D883716_9274_0-1.parquet?sp=r&st=2022-08-04T15:31:55Z&se=2022-08-06T23:31:55Z&spr=https&sv=2021-06-08&sr=b&sig=bogus"));
        edgesParquetUrls =
            List.of(
                new URL(
                    "https://mysnapshotsa.blob.core.windows.net/metadata/parquet/9516afec-583f-11ec-bf63-0242ac130002/edges.parquet/F0EE365B-314D-4E19-A177-E8F63D883716_9274_0-1.parquet?sp=r&st=2022-08-04T15:31:55Z&se=2022-08-06T23:31:55Z&spr=https&sv=2021-06-08&sr=b&sig=bogus"),
                new URL(
                    "https://mysnapshotsa.blob.core.windows.net/metadata/parquet/9516afec-583f-11ec-bf63-0242ac130002/edges.parquet/F0EE365B-314D-4E19-A177-E8F63D883716_9274_0-2.parquet?sp=r&st=2022-08-04T15:31:55Z&se=2022-08-06T23:31:55Z&spr=https&sv=2021-06-08&sr=b&sig=bogus"));
        testResultParquetUrls =
            List.of(
                new URL(
                    "https://mysnapshotsa.blob.core.windows.net/metadata/parquet/9516afec-583f-11ec-bf63-0242ac130002/test_result.parquet/F0EE365B-314D-4E19-A177-E8F63D883716_9274_0-1.parquet?sp=r&st=2022-08-04T15:31:55Z&se=2022-08-06T23:31:55Z&spr=https&sv=2021-06-08&sr=b&sig=bogus"));

      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
