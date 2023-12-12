package org.databiosphere.workspacedataservice.dataimport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import bio.terra.datarepo.model.RelationshipModel;
import bio.terra.datarepo.model.RelationshipTermModel;
import bio.terra.datarepo.model.SnapshotExportResponseModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.model.TdrManifestImportTable;
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
  @MockBean BatchWriteService batchWriteService;
  @MockBean ActivityLogger activityLogger;
  @Autowired RestClientRetry restClientRetry;
  @Autowired ObjectMapper objectMapper;

  // test resources used below
  @Value("classpath:tdrmanifest/azure_small.json")
  Resource manifestAzure;

  @Value("classpath:tdrmanifest/tdr_response_with_cycle.json")
  Resource manifestWithCycle;

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

    // this manifest describes tables for project, edges, test_result, genome in the snapshot
    // schema,
    // but only contains export data files for project, edges, and test_result.
    List<TdrManifestImportTable> expected =
        List.of(
            // single primary key
            new TdrManifestImportTable(
                RecordType.valueOf("project"),
                "project_id",
                List.of(
                    new URL(
                        "https://mysnapshotsa.blob.core.windows.net/metadata/parquet/9516afec-583f-11ec-bf63-0242ac130002/project.parquet/F0EE365B-314D-4E19-A177-E8F63D883716_9274_0-1.parquet?sp=r&st=2022-08-04T15:31:55Z&se=2022-08-06T23:31:55Z&spr=https&sv=2021-06-08&sr=b&sig=bogus")),
                List.of()),
            // null primary key
            new TdrManifestImportTable(
                RecordType.valueOf("edges"),
                "datarepo_row_id",
                List.of(
                    new URL(
                        "https://mysnapshotsa.blob.core.windows.net/metadata/parquet/9516afec-583f-11ec-bf63-0242ac130002/edges.parquet/F0EE365B-314D-4E19-A177-E8F63D883716_9274_0-1.parquet?sp=r&st=2022-08-04T15:31:55Z&se=2022-08-06T23:31:55Z&spr=https&sv=2021-06-08&sr=b&sig=bogus"),
                    new URL(
                        "https://mysnapshotsa.blob.core.windows.net/metadata/parquet/9516afec-583f-11ec-bf63-0242ac130002/edges.parquet/F0EE365B-314D-4E19-A177-E8F63D883716_9274_0-2.parquet?sp=r&st=2022-08-04T15:31:55Z&se=2022-08-06T23:31:55Z&spr=https&sv=2021-06-08&sr=b&sig=bogus")),
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
                List.of(
                    new URL(
                        "https://mysnapshotsa.blob.core.windows.net/metadata/parquet/9516afec-583f-11ec-bf63-0242ac130002/test_result.parquet/F0EE365B-314D-4E19-A177-E8F63D883716_9274_0-1.parquet?sp=r&st=2022-08-04T15:31:55Z&se=2022-08-06T23:31:55Z&spr=https&sv=2021-06-08&sr=b&sig=bogus")),
                List.of()));

    List<TdrManifestImportTable> actual =
        tdrManifestQuartzJob.extractTableInfo(snapshotExportResponseModel);

    assertEquals(expected, actual);
  }
}
