package org.databiosphere.workspacedataservice.dataimport.tdr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.TestTags.SLOW;
import static org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestTestUtils.stubJobContext;
import static org.databiosphere.workspacedataservice.sam.SamAuthorizationDao.WORKSPACE_ROLES;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.datarepo.model.RelationshipModel;
import bio.terra.datarepo.model.RelationshipTermModel;
import bio.terra.datarepo.model.SnapshotExportResponseModel;
import bio.terra.workspace.model.ResourceList;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.dataimport.FileDownloadHelper;
import org.databiosphere.workspacedataservice.dataimport.ImportDetails;
import org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestExemplarData.AzureSmall;
import org.databiosphere.workspacedataservice.recordsink.RawlsAttributePrefixer.PrefixStrategy;
import org.databiosphere.workspacedataservice.recordsink.RecordSink;
import org.databiosphere.workspacedataservice.recordsink.RecordSinkFactory;
import org.databiosphere.workspacedataservice.recordsource.RecordSource.ImportMode;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.RecordService;
import org.databiosphere.workspacedataservice.service.model.TdrManifestImportTable;
import org.databiosphere.workspacedataservice.service.model.exception.TdrManifestImportException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.Resource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@DirtiesContext
@SpringBootTest
@ActiveProfiles(profiles = {"mock-sam"})
class TdrManifestQuartzJobTest extends TestBase {

  @MockBean JobDao jobDao;
  @MockBean WorkspaceManagerDao wsmDao;

  @MockBean CollectionService collectionService;
  @MockBean ActivityLogger activityLogger;
  @MockBean RecordService recordService;
  @MockBean RecordDao recordDao;
  @MockBean DataImportProperties dataImportProperties;
  @MockBean SamDao samDao;
  @Autowired RecordSinkFactory recordSinkFactory;
  @Autowired RestClientRetry restClientRetry;
  @Autowired ObjectMapper objectMapper;
  @Autowired TdrTestSupport testSupport;

  // test resources used below
  @Value("classpath:tdrmanifest/azure_small.json")
  Resource manifestAzure;

  @Value("classpath:tdrmanifest/tdr_response_with_cycle.json")
  Resource manifestWithCycle;

  @Value("classpath:parquet/empty.parquet")
  Resource emptyParquet;

  @Value("classpath:parquet/malformed.parquet")
  Resource malformedParquet;

  @Value("classpath:tdrmanifest/v2f.json")
  Resource v2fManifestResource;

  // this one contains properties not defined in the Java models
  @Value("classpath:tdrmanifest/extra_properties.json")
  Resource manifestWithUnknownProperties;

  @BeforeEach
  void beforeEach() {
    // set up recordService and recordDao to be noops
    when(recordService.addOrUpdateColumnIfNeeded(any(), any(), any(), any(), any()))
        .thenReturn(Map.of());
    doNothing().when(recordService).batchUpsert(any(), any(), any(), any(), any());

    when(recordDao.recordTypeExists(any(), any())).thenReturn(false);
    doNothing().when(recordDao).createRecordType(any(), any(), any(), any(), any());
  }

  @Test
  void extractSnapshotInfo() throws IOException {
    UUID workspaceId = UUID.randomUUID();
    TdrManifestQuartzJob tdrManifestQuartzJob = testSupport.buildTdrManifestQuartzJob();
    SnapshotExportResponseModel snapshotExportResponseModel =
        tdrManifestQuartzJob.parseManifest(manifestAzure.getURI());

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
        tdrManifestQuartzJob.extractTableInfo(
            snapshotExportResponseModel, WorkspaceId.of(workspaceId));

    assertEquals(expected, actual);
  }

  @Test
  void parseEmptyParquet() throws IOException {
    UUID workspaceId = UUID.randomUUID();
    TdrManifestQuartzJob tdrManifestQuartzJob = testSupport.buildTdrManifestQuartzJob();
    TdrManifestImportTable table =
        new TdrManifestImportTable(
            RecordType.valueOf("data"),
            "datarepo_row_id",
            List.of(emptyParquet.getURL()),
            List.of());

    // An empty file should not throw any errors
    FileDownloadHelper fileMap =
        assertDoesNotThrow(() -> tdrManifestQuartzJob.getFilesForImport(List.of(table)));
    assert (fileMap.getFileMap().isEmpty());
  }

  /*
   * the TDR manifest JSON changes not-infrequently. When TDR adds fields, are we resilient to those
   * additions?
   */
  @Test
  void parseUnknownFieldsInManifest() {
    UUID workspaceId = UUID.randomUUID();
    TdrManifestQuartzJob tdrManifestQuartzJob = testSupport.buildTdrManifestQuartzJob();
    SnapshotExportResponseModel snapshotExportResponseModel =
        assertDoesNotThrow(
            () -> tdrManifestQuartzJob.parseManifest(manifestWithUnknownProperties.getURI()));

    // smoke-test that it parsed correctly
    assertEquals(
        UUID.fromString("00000000-1111-2222-3333-444455556666"),
        snapshotExportResponseModel.getSnapshot().getId());
  }

  @Test
  void parseMalformedParquet() throws IOException {
    UUID workspaceId = UUID.randomUUID();
    UUID jobId = UUID.randomUUID();
    Supplier<String> emailSupplier = () -> "testEmail";
    TdrManifestQuartzJob tdrManifestQuartzJob = testSupport.buildTdrManifestQuartzJob();
    TdrManifestImportTable table =
        new TdrManifestImportTable(
            RecordType.valueOf("data"),
            "datarepo_row_id",
            List.of(malformedParquet.getURL()),
            List.of());

    InputFile malformedFile =
        HadoopInputFile.fromPath(
            new Path(malformedParquet.getURL().toString()), new Configuration());

    ImportDetails importDetails =
        new ImportDetails(
            jobId,
            emailSupplier,
            WorkspaceId.of(workspaceId),
            CollectionId.of(workspaceId),
            PrefixStrategy.TDR,
            new TdrManifestJobInput(
                URI.create("https://data.terra.bio/test.manifest"),
                new TdrManifestImportOptions(false)));
    try (RecordSink recordSink = recordSinkFactory.buildRecordSink(importDetails)) {

      // Make sure real errors on parsing parquets are not swallowed
      assertThrows(
          TdrManifestImportException.class,
          () ->
              tdrManifestQuartzJob.importTable(
                  malformedFile, table, recordSink, ImportMode.BASE_ATTRIBUTES));
    }
  }

  @Test
  @Tag(SLOW)
  void useWorkspaceIdFromCollection() throws IOException {
    // ARRANGE
    // set up ids
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    UUID jobId = UUID.randomUUID();
    // mock collection service to return this workspace id for this collection id
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);
    // WSM should report no snapshots already linked to this workspace
    // note that if enumerateDataRepoSnapshotReferences is called with the wrong workspaceId,
    // this test will fail
    when(wsmDao.enumerateDataRepoSnapshotReferences(eq(workspaceId.id()), anyInt(), anyInt()))
        .thenReturn(new ResourceList());

    // set up the job
    TdrManifestQuartzJob tdrManifestQuartzJob = testSupport.buildTdrManifestQuartzJob();
    JobExecutionContext mockContext = stubJobContext(jobId, v2fManifestResource, collectionId.id());

    // ACT
    // execute the job
    tdrManifestQuartzJob.executeInternal(jobId, mockContext);

    // ASSERT
    verify(wsmDao).enumerateDataRepoSnapshotReferences(eq(workspaceId.id()), anyInt(), anyInt());
  }

  @Test
  @Tag(SLOW)
  /* note: this functionality is control plane only.*/
  void testSyncPermissions() throws IOException {
    // ARRANGE
    // set up ids
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    CollectionId collectionId = CollectionId.of(UUID.randomUUID());
    UUID jobId = UUID.randomUUID();
    // snapshotId of the v2fManifestResource
    UUID snapshotId = UUID.fromString("e3638824-9ed9-408e-b3f5-cba7585658a3");
    // mock collection service to return this workspace id for this collection id
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);
    // WSM should report no snapshots already linked to this workspace
    // note that if enumerateDataRepoSnapshotReferences is called with the wrong workspaceId,
    // this test will fail
    when(wsmDao.enumerateDataRepoSnapshotReferences(eq(workspaceId.id()), anyInt(), anyInt()))
        .thenReturn(new ResourceList());
    // mock property that only gets enabled in application-control-plane.yml
    when(dataImportProperties.isTdrPermissionSyncingEnabled()).thenReturn(true);
    // mock sam call
    doNothing().when(samDao).addWorkspacePoliciesAsSnapshotReader(any(), any(), anyString());

    // set up the job
    TdrManifestQuartzJob tdrManifestQuartzJob = testSupport.buildTdrManifestQuartzJob();
    JobExecutionContext mockContext =
        stubJobContext(jobId, v2fManifestResource, collectionId.id(), /* syncPermissions= */ true);

    // ACT
    // execute the job
    tdrManifestQuartzJob.executeInternal(jobId, mockContext);

    // ASSERT
    ArgumentCaptor<String> roleCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<WorkspaceId> workspaceCaptor = ArgumentCaptor.forClass(WorkspaceId.class);
    ArgumentCaptor<UUID> snapshotIdCaptor = ArgumentCaptor.forClass(UUID.class);
    verify(samDao, times(4))
        .addWorkspacePoliciesAsSnapshotReader(
            workspaceCaptor.capture(), snapshotIdCaptor.capture(), roleCaptor.capture());
    assertThat(roleCaptor.getAllValues()).containsExactlyInAnyOrder(WORKSPACE_ROLES);
    assertThat(workspaceCaptor.getAllValues()).containsOnly(workspaceId).hasSize(4);
    assertThat(snapshotIdCaptor.getAllValues()).containsOnly(snapshotId).hasSize(4);
  }
}
