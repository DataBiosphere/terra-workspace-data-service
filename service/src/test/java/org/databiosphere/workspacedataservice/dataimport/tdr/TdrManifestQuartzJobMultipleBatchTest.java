package org.databiosphere.workspacedataservice.dataimport.tdr;

import static org.databiosphere.workspacedataservice.TestTags.SLOW;
import static org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestTestUtils.stubJobContext;
import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.TDRMANIFEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dataimport.ImportValidator;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.ImportService;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.workspace.DataTableTypeInspector;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/**
 * Tests for TdrManifest import that execute "end-to-end" and involve multiple batches. These tests
 * ensure multiple batches by setting twds.write.batch.size very low.
 *
 * <p>See also TdrManifestQuartzJobE2ETest for additional test coverage.
 *
 * <p>Notably stubbed out: the parquet files referenced in the manifests come from the classpath (as
 * indicated by a "classpath:" prefix) rather than being fetched from a remote URL. This is made
 * possible by using TdrTestSupport.buildTdrManifestQuartzJob() which creates a special
 * TdrManifestQuartzJob that knows how to fetch from the classpath.
 */
@ActiveProfiles(profiles = {"mock-sam", "noop-scheduler-dao"})
@DirtiesContext
@SpringBootTest
@AutoConfigureMockMvc
@TestPropertySource(properties = {"twds.write.batch.size=2"})
// TODO AJ-2004: move to the control-plane profile once AJ-2004 is resolved
class TdrManifestQuartzJobMultipleBatchTest extends DataPlaneTestBase {
  @Autowired private RecordOrchestratorService recordOrchestratorService;
  @Autowired private ImportService importService;
  @Autowired private CollectionService collectionService;
  @Autowired private TdrTestSupport testSupport;
  @Autowired private NamedParameterJdbcTemplate namedTemplate;
  @Autowired private TwdsProperties twdsProperties;

  // Mock ImportValidator to allow importing test data from a file:// URL.
  @MockitoBean ImportValidator importValidator;
  @MockitoBean RawlsClient rawlsClient;
  @MockitoBean DataTableTypeInspector dataTableTypeInspector;

  @Value("classpath:tdrmanifest/with-entity-reference-lists.json")
  Resource withEntityReferenceListsResource;

  UUID collectionId;

  @BeforeEach
  void beforeEach() {
    CollectionServerModel collectionServerModel =
        TestUtils.createCollection(collectionService, twdsProperties.workspaceId());
    collectionId = collectionServerModel.getId();
    // dataTableTypeInspector says ok to use data tables
    when(dataTableTypeInspector.getWorkspaceDataTableType(any()))
        .thenReturn(WorkspaceDataTableType.WDS);
  }

  @AfterEach
  void afterEach() {
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
  }

  // This test targets AJ-1909, which concerns a SQL exception thrown when importing, wherein
  // we retried to re-add the primary key column a second time. The second attempt would fail
  // because the column already existed.
  // The import will fail if the exception is present. This test verifies the import succeeds,
  // and doesn't verify too much else about the import results.
  @Test
  @Tag(SLOW)
  void defaultPrimaryKey() throws IOException, JobExecutionException {
    var importResource = withEntityReferenceListsResource;

    var importRequest = new ImportRequestServerModel(TDRMANIFEST, importResource.getURI());

    // because we have a mock scheduler dao, this won't trigger Quartz
    var genericJobServerModel = importService.createImport(collectionId, importRequest);

    UUID jobId = genericJobServerModel.getJobId();
    JobExecutionContext mockContext = stubJobContext(jobId, importResource, collectionId);

    testSupport.buildTdrManifestQuartzJob().execute(mockContext);

    List<RecordTypeSchema> allTypes =
        recordOrchestratorService.describeAllRecordTypes(collectionId, "v0.2");

    Map<String, Integer> actualCounts =
        allTypes.stream()
            .collect(
                Collectors.toMap(
                    recordTypeSchema -> recordTypeSchema.name().getName(),
                    RecordTypeSchema::count));

    Map<String, Integer> expectedCounts =
        new ImmutableMap.Builder<String, Integer>().put("sample", 5).put("person", 3).build();

    assertEquals(expectedCounts, actualCounts);
  }
}
