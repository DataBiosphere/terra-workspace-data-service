package org.databiosphere.workspacedataservice.dataimport.tdr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.TestTags.SLOW;
import static org.databiosphere.workspacedataservice.dataimport.tdr.TdrManifestTestUtils.stubJobContext;
import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.TDRMANIFEST;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.when;

import bio.terra.workspace.model.ResourceList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dataimport.ImportValidator;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.ImportService;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
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
@ActiveProfiles(profiles = {"mock-sam", "noop-scheduler-dao"})
@DirtiesContext
@SpringBootTest
@AutoConfigureMockMvc
class TdrManifestQuartzJobE2ETest extends DataPlaneTestBase {
  @Autowired private RecordOrchestratorService recordOrchestratorService;
  @Autowired private ImportService importService;
  @Autowired private CollectionService collectionService;
  @Autowired private TdrTestSupport testSupport;
  @Autowired private NamedParameterJdbcTemplate namedTemplate;
  @Autowired private TwdsProperties twdsProperties;

  // Mock ImportValidator to allow importing test data from a file:// URL.
  @MockBean ImportValidator importValidator;
  @MockBean WorkspaceManagerDao wsmDao;

  @Value("classpath:tdrmanifest/v2f.json")
  Resource v2fManifestResource;

  @Value("classpath:tdrmanifest/with-entity-reference-lists.json")
  Resource withEntityReferenceListsResource;

  UUID collectionId;

  @BeforeEach
  void beforeEach() {
    CollectionServerModel collectionServerModel =
        TestUtils.createCollection(collectionService, twdsProperties.workspaceId());
    collectionId = collectionServerModel.getId();
  }

  @AfterEach
  void afterEach() {
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
  }

  @Test
  @Tag(SLOW)
  void importV2FManifest() throws IOException, JobExecutionException {
    var importRequest = new ImportRequestServerModel(TDRMANIFEST, v2fManifestResource.getURI());

    // because we have a mock scheduler dao, this won't trigger Quartz
    var genericJobServerModel = importService.createImport(collectionId, importRequest);

    UUID jobId = genericJobServerModel.getJobId();
    JobExecutionContext mockContext = stubJobContext(jobId, v2fManifestResource, collectionId);

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());

    testSupport.buildTdrManifestQuartzJob().execute(mockContext);

    List<RecordTypeSchema> allTypes =
        recordOrchestratorService.describeAllRecordTypes(collectionId, "v0.2");

    // spot-check some column data types to see if they are good
    // TODO (AJ-1536): types aren't correct yet!  Need to respect the schema provided in the
    //   manifest rather than trusting Parquet/Avro's inferred schema.
    assertDataType(allTypes, "all_data_types", "bool_column", DataTypeMapping.BOOLEAN);
    assertDataType(
        allTypes,
        "all_data_types",
        "date_column",
        DataTypeMapping.DATE); // actual data: 19174 (days since epoch)

    assertDataType(
        allTypes,
        "all_data_types",
        "date_array_column",
        DataTypeMapping
            .STRING); // actual data is string: "[\"2022-7-02\",\"2022-7-03\",\"2022-7-04\"]"
    assertDataType(
        allTypes,
        "all_data_types",
        "date_time_column",
        DataTypeMapping.STRING); // actual data is string: "2022-07-05T12:00:01Z[UTC]"
    assertDataType(
        allTypes,
        "all_data_types",
        "date_time_array_column",
        DataTypeMapping.STRING); // actual data is string: "[\"2022-07-05 12:00:01\"]"
    assertDataType(
        allTypes,
        "all_data_types",
        "dir_ref_column",
        DataTypeMapping.STRING); // actual data is null
    assertDataType(
        allTypes,
        "all_data_types",
        "file_ref_column",
        DataTypeMapping.STRING); // actual data is null
    assertDataType(allTypes, "all_data_types", "float_column", DataTypeMapping.NUMBER);
    assertDataType(
        allTypes,
        "all_data_types",
        "float_array_column",
        DataTypeMapping.STRING); // actual data is string: "[1.232,123.0]"
    assertDataType(allTypes, "all_data_types", "float64_column", DataTypeMapping.NUMBER);
    assertDataType(
        allTypes,
        "all_data_types",
        "float64_array_column",
        DataTypeMapping.STRING); // actual data is string: "[1.232,123.0]"
    assertDataType(allTypes, "all_data_types", "int_column", DataTypeMapping.NUMBER);
    assertDataType(
        allTypes,
        "all_data_types",
        "int_array_column",
        DataTypeMapping.STRING); // actual data is string: "[245,3325,2343]"
    assertDataType(
        allTypes,
        "all_data_types",
        "int64_column",
        DataTypeMapping.STRING); // actual data is number: 234235 (???)
    assertDataType(
        allTypes,
        "all_data_types",
        "int64_array_column",
        DataTypeMapping.STRING); // actual data is string: "[1,234235]"
    assertDataType(allTypes, "all_data_types", "numeric_column", DataTypeMapping.NUMBER);
    assertDataType(
        allTypes,
        "all_data_types",
        "numeric_array_column",
        DataTypeMapping.STRING); // actual data is string: "[2,2,3]"
    assertDataType(
        allTypes,
        "all_data_types",
        "time_column",
        DataTypeMapping.STRING); // actual data is string: "1970-01-02T23:59:59.999Z[UTC]"
    assertDataType(
        allTypes,
        "all_data_types",
        "time_array_column",
        DataTypeMapping.STRING); // actual data is string: "[\"1:34:01\",\"12:34:01.0908\"]"
    assertDataType(
        allTypes,
        "all_data_types",
        "timestamp_column",
        DataTypeMapping.STRING); // actual data is string: "2023-06-01T01:02:40Z[UTC]"
    assertDataType(
        allTypes,
        "all_data_types",
        "timestamp_array_column",
        DataTypeMapping.STRING); // actual data is string: "[\"2023-06-02 01:04:40\"]"

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

  @Test
  @Tag(SLOW)
  void withEntityReferenceListsManifest() throws IOException, JobExecutionException {
    var importRequest =
        new ImportRequestServerModel(TDRMANIFEST, withEntityReferenceListsResource.getURI());

    // because we have a mock scheduler dao, this won't trigger Quartz
    var genericJobServerModel = importService.createImport(collectionId, importRequest);

    UUID jobId = genericJobServerModel.getJobId();
    JobExecutionContext mockContext =
        stubJobContext(jobId, withEntityReferenceListsResource, collectionId);

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());

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
        new ImmutableMap.Builder<String, Integer>().put("person", 3).put("sample", 5).build();

    assertEquals(expectedCounts, actualCounts);

    // spot-check that the person type references the sample type
    RecordResponse recordResponse =
        recordOrchestratorService.getSingleRecord(
            collectionId, "v0.2", RecordType.valueOf("person"), "1");
    Object actual = recordResponse.recordAttributes().getAttributeValue("samples");
    String[] actualArray = assertInstanceOf(String[].class, actual);
    assertArrayEquals(new String[] {"terra-wds:/sample/1", "terra-wds:/sample/2"}, actualArray);
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
