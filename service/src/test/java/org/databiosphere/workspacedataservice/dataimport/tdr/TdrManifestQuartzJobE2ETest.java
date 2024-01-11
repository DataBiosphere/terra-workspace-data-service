package org.databiosphere.workspacedataservice.dataimport.tdr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.TestTags.SLOW;
import static org.databiosphere.workspacedataservice.dataimport.pfb.PfbTestUtils.stubJobContext;
import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.TDRMANIFEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import bio.terra.workspace.model.ResourceList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.databiosphere.workspacedataservice.dao.SchedulerDao;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.ImportService;
import org.databiosphere.workspacedataservice.service.InstanceService;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.Resource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

/**
 * Tests for TdrManifest import that execute "end-to-end" - that is, they go through the whole
 * process of parsing the parquet file, creating tables in Postgres, inserting rows, and then
 * reading back the rows/counts/schema from Postgres.
 *
 * <p>Notably stubbed out: the parquet files referenced in the manifests come from the classpath (as
 * indicated by a "classpath:" prefix) rather than being fetched from a remote URL. This is made
 * possible by using TdrTestSupport.buildTdrManifestQuartzJob() which creates a special
 * TdrManifestQuartzJob that knows how to fetch from the classpath.
 */
@ActiveProfiles(profiles = "mock-sam")
@DirtiesContext
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@AutoConfigureMockMvc
public class TdrManifestQuartzJobE2ETest {
  @Autowired private RecordOrchestratorService recordOrchestratorService;
  @Autowired private ImportService importService;
  @Autowired private InstanceService instanceService;
  @Autowired private TdrTestSupport testSupport;

  @MockBean SchedulerDao schedulerDao;
  @MockBean WorkspaceManagerDao wsmDao;

  @Value("classpath:tdrmanifest/v2f.json")
  Resource v2fManifestResource;

  UUID instanceId;

  @BeforeAll
  void beforeAll() {
    doNothing().when(schedulerDao).schedule(any());
  }

  @BeforeEach
  void beforeEach() {
    instanceId = UUID.randomUUID();
    instanceService.createInstance(instanceId, "v0.2");
  }

  @AfterEach
  void afterEach() {
    instanceService.deleteInstance(instanceId, "v0.2");
  }

  @Test
  @Tag(SLOW)
  void importV2FManifest() throws IOException, JobExecutionException {
    var importRequest = new ImportRequestServerModel(TDRMANIFEST, v2fManifestResource.getURI());

    // because we have a mock scheduler dao, this won't trigger Quartz
    var genericJobServerModel = importService.createImport(instanceId, importRequest);

    UUID jobId = genericJobServerModel.getJobId();
    JobExecutionContext mockContext = stubJobContext(jobId, v2fManifestResource, instanceId);

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());

    testSupport
        .buildTdrManifestQuartzJob(/* workspaceId= */ UUID.randomUUID())
        .execute(mockContext);

    List<RecordTypeSchema> allTypes =
        recordOrchestratorService.describeAllRecordTypes(instanceId, "v0.2");

    // spot-check some column data types to see if they are good
    assertDataType(allTypes, "ancestry_specific_meta_analysis", "ancestry", DataTypeMapping.STRING);
    assertDataType(allTypes, "ancestry_specific_meta_analysis", "beta", DataTypeMapping.NUMBER);
    assertDataType(
        allTypes, "ancestry_specific_meta_analysis", "datarepo_row_id", DataTypeMapping.STRING);

    // TODO: beef up test coverage on the various types represented by the fixture

    Map<String, Integer> actualCounts =
        allTypes.stream()
            .collect(
                Collectors.toMap(
                    recordTypeSchema -> recordTypeSchema.name().getName(),
                    RecordTypeSchema::count));

    Map<String, Integer> expectedCounts =
        new ImmutableMap.Builder<String, Integer>()
            .put("ancestry_specific_meta_analysis", 1000)
            .put("frequency_analysis", 1003)
            .put("transcript_consequence", 1003)
            .put("variant", 1004)
            .put("all_data_types", 5)
            .put("feature_consequence", 1003)
            .build();

    assertEquals(expectedCounts, actualCounts);
  }

  private void assertDataType(
      List<RecordTypeSchema> allTypes,
      String typeName,
      String attributeName,
      DataTypeMapping expectedType) {

    Optional<RecordTypeSchema> maybeRecordTypeSchema =
        allTypes.stream().filter(x -> x.name().getName().equals(typeName)).findFirst();
    assertThat(maybeRecordTypeSchema).isNotEmpty();

    List<AttributeSchema> allAttributes = maybeRecordTypeSchema.get().attributes();
    Optional<AttributeSchema> maybeAttributeSchema =
        allAttributes.stream().filter(x -> x.name().equals(attributeName)).findFirst();
    assertThat(maybeAttributeSchema).isNotEmpty();

    assertEquals(expectedType.name(), maybeAttributeSchema.get().datatype());
  }
}
