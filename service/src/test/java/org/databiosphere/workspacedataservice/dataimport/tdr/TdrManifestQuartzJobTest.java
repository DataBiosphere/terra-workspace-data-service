package org.databiosphere.workspacedataservice.dataimport.tdr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.datarepo.model.RelationshipModel;
import bio.terra.datarepo.model.RelationshipTermModel;
import bio.terra.datarepo.model.SnapshotExportResponseModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestExemplarData.AzureSmall;
import org.databiosphere.workspacedataservice.recordstream.TwoPassStreamingWriteHandler;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.model.BatchWriteResult;
import org.databiosphere.workspacedataservice.service.model.TdrManifestImportTable;
import org.databiosphere.workspacedataservice.service.model.exception.TdrManifestImportException;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.Resource;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest
public class TdrManifestQuartzJobTest {

  @MockBean JobDao jobDao;
  @MockBean WorkspaceManagerDao wsmDao;
  @Autowired BatchWriteService batchWriteService;
  @MockBean ActivityLogger activityLogger;
  @Autowired RestClientRetry restClientRetry;
  @Autowired ObjectMapper objectMapper;

  // test resources used below
  @Value("classpath:tdrmanifest/azure_small.json")
  Resource manifestAzure;

  @Value("classpath:tdrmanifest/tdr_response_with_cycle.json")
  Resource manifestWithCycle;

  @Value("classpath:parquet/empty.parquet")
  Resource emptyParquet;

  @Value("classpath:parquet/malformed.parquet")
  Resource malformedParquet;

  // this one contains properties not defined in the Java models
  @Value("classpath:tdrmanifest/extra_properties.json")
  Resource manifestWithUnknownProperties;

  @Test
  void extractSnapshotInfo() throws IOException {
    UUID workspaceId = UUID.randomUUID();
    TdrManifestQuartzJob tdrManifestQuartzJob =
        new TdrManifestQuartzJob(
            jobDao,
            wsmDao,
            restClientRetry,
            batchWriteService,
            activityLogger,
            workspaceId,
            objectMapper);

    SnapshotExportResponseModel snapshotExportResponseModel =
        tdrManifestQuartzJob.parseManifest(manifestAzure.getURL());

    // this manifest describes tables for project, edges, test_result, genome in the snapshot,
    // but only contains export data files for project, edges, and test_result.
    List<TdrManifestImportTable> expected =
        List.of(
            // single primary key
            new TdrManifestImportTable(
                RecordType.valueOf("project"),
                "project_id",
                AzureSmall.projectParquetUrls,
                List.of()),
            // null primary key
            new TdrManifestImportTable(
                RecordType.valueOf("edges"),
                "datarepo_row_id",
                AzureSmall.edgesParquetUrls,
                List.of(
                    new RelationshipModel()
                        .name("from_edges.project_id_to_project.project_id")
                        .from(new RelationshipTermModel().table("edges").column("project_id"))
                        .to(new RelationshipTermModel().table("project").column("project_id")))),
            // compound primary key. Also has a relation listed in the manifest, but with an invalid
            // target
            new TdrManifestImportTable(
                RecordType.valueOf("test_result"),
                "datarepo_row_id",
                AzureSmall.testResultParquetUrls,
                List.of()));

    List<TdrManifestImportTable> actual =
        tdrManifestQuartzJob.extractTableInfo(snapshotExportResponseModel);

    assertEquals(expected, actual);
  }

  @Test
  void parseEmptyParquet() throws IOException {
    UUID workspaceId = UUID.randomUUID();
    TdrManifestQuartzJob tdrManifestQuartzJob =
        new TdrManifestQuartzJob(
            jobDao,
            wsmDao,
            restClientRetry,
            batchWriteService,
            activityLogger,
            workspaceId,
            objectMapper);

    TdrManifestImportTable table =
        new TdrManifestImportTable(
            RecordType.valueOf("data"),
            "datarepo_row_id",
            List.of(emptyParquet.getURL()),
            List.of());

    // An empty file should not throw any errors
    BatchWriteResult actual =
        assertDoesNotThrow(
            () ->
                tdrManifestQuartzJob.importTable(
                    emptyParquet.getURL(),
                    table,
                    workspaceId,
                    TwoPassStreamingWriteHandler.ImportMode.BASE_ATTRIBUTES));
    assertThat(actual.entrySet()).isEmpty();
  }

  /*
   * the TDR manifest JSON changes not-infrequently. When TDR adds fields, are we resilient to those
   * additions?
   */
  @Test
  void parseUnknownFieldsInManifest() throws IOException {
    UUID workspaceId = UUID.randomUUID();
    TdrManifestQuartzJob tdrManifestQuartzJob =
        new TdrManifestQuartzJob(
            jobDao,
            wsmDao,
            restClientRetry,
            batchWriteService,
            activityLogger,
            workspaceId,
            objectMapper);

    TdrManifestImportTable table =
        new TdrManifestImportTable(
            RecordType.valueOf("data"),
            "datarepo_row_id",
            List.of(emptyParquet.getURL()),
            List.of());

    SnapshotExportResponseModel snapshotExportResponseModel =
        assertDoesNotThrow(
            () -> tdrManifestQuartzJob.parseManifest(manifestWithUnknownProperties.getURL()));

    // smoke-test that it parsed correctly
    assertEquals(
        UUID.fromString("00000000-1111-2222-3333-444455556666"),
        snapshotExportResponseModel.getSnapshot().getId());
  }

  @Test
  void parseMalformedParquet() throws IOException {
    UUID workspaceId = UUID.randomUUID();

    TdrManifestQuartzJob tdrManifestQuartzJob =
        new TdrManifestQuartzJob(
            jobDao,
            wsmDao,
            restClientRetry,
            batchWriteService,
            activityLogger,
            workspaceId,
            objectMapper);

    TdrManifestImportTable table =
        new TdrManifestImportTable(
            RecordType.valueOf("data"),
            "datarepo_row_id",
            List.of(malformedParquet.getURL()),
            List.of());

    // Make sure real errors on parsing parquets are not swallowed
    assertThrows(
        TdrManifestImportException.class,
        () ->
            tdrManifestQuartzJob.importTable(
                manifestAzure.getURL(),
                table,
                workspaceId,
                TwoPassStreamingWriteHandler.ImportMode.BASE_ATTRIBUTES));
  }
}