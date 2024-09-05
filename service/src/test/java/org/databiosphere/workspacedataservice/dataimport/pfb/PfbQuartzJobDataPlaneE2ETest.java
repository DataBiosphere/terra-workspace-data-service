package org.databiosphere.workspacedataservice.dataimport.pfb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.TestTags.SLOW;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.workspace.model.ResourceList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.math.BigDecimal;
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
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.service.RelationUtils;
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
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

/**
 * Tests for PFB import that execute "end-to-end" - that is, they go through the whole process of
 * parsing the PFB, creating tables in Postgres, inserting rows, and then reading back the
 * rows/counts/schema from Postgres.
 */
@ActiveProfiles(profiles = {"mock-sam", "noop-scheduler-dao"})
@DirtiesContext
@SpringBootTest
class PfbQuartzJobDataPlaneE2ETest extends DataPlaneTestBase {
  @Autowired TwdsProperties twdsProperties;
  @Autowired RecordOrchestratorService recordOrchestratorService;
  @Autowired CollectionService collectionService;
  @Autowired PfbTestSupport testSupport;
  @Autowired NamedParameterJdbcTemplate namedTemplate;
  @MockBean WorkspaceManagerDao wsmDao;
  // Mock ImportValidator to allow importing test data from a file:// URL.
  @MockBean ImportValidator importValidator;
  @SpyBean SamDao samDao;

  // test resources used below
  @Value("classpath:avro/four_rows.avro")
  Resource fourRowsAvroResource;

  @Value("classpath:avro/test.avro")
  Resource testAvroResource;

  @Value("classpath:avro/precision.avro")
  Resource testPrecisionResource;

  @Value("classpath:avro/forward_relations.avro")
  Resource forwardRelationsAvroResource;

  @Value("classpath:avro/cyclical.avro")
  Resource cyclicalAvroResource;

  UUID collectionId;

  @BeforeEach
  void beforeEach() {
    CollectionServerModel collectionServerModel =
        TestUtils.createCollection(collectionService, twdsProperties.workspaceId());
    collectionId = collectionServerModel.getId();

    // stub out WSM to report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());
  }

  @AfterEach
  void afterEach() {
    TestUtils.cleanAllCollections(collectionService, namedTemplate);
  }

  /* import test.avro, and validate the tables and row counts it imported. */
  @Test
  @Tag(SLOW)
  void importTestResource() throws IOException, JobExecutionException {
    testSupport.executePfbImportQuartzJob(collectionId, testAvroResource);

    /* the testAvroResource should insert:
       - 3202 record(s) of type activities
       - 3202 record(s) of type files
       - 3202 record(s) of type donors
       - 3202 record(s) of type biosamples
       - 1 record(s) of type datasets
    */

    List<RecordTypeSchema> allTypes =
        recordOrchestratorService.describeAllRecordTypes(collectionId, "v0.2");

    // spot-check some column data types to see if they are good
    assertDataType(allTypes, "activities", "activity_type", DataTypeMapping.ARRAY_OF_STRING);
    assertDataType(allTypes, "files", "file_format", DataTypeMapping.STRING);
    assertDataType(allTypes, "files", "file_size", DataTypeMapping.NUMBER);
    assertDataType(allTypes, "files", "is_supplementary", DataTypeMapping.BOOLEAN);

    Map<String, Integer> actualCounts =
        allTypes.stream()
            .collect(
                Collectors.toMap(
                    recordTypeSchema -> recordTypeSchema.name().getName(),
                    RecordTypeSchema::count));

    Map<String, Integer> expectedCounts =
        new ImmutableMap.Builder<String, Integer>()
            .put("activities", 3202)
            .put("files", 3202)
            .put("donors", 3202)
            .put("biosamples", 3202)
            .put("datasets", 1)
            .build();

    assertEquals(expectedCounts, actualCounts);
  }

  /* import four_rows.avro, and validate the tables and row counts it imported. */
  @Test
  void importFourRowsResource() throws IOException, JobExecutionException {
    testSupport.executePfbImportQuartzJob(collectionId, fourRowsAvroResource);

    /* the fourRowsAvroResource should insert:
       - 3 record(s) of type data_release
       - 1 record(s) of type submitted_aligned_reads
    */
    List<RecordTypeSchema> allTypes =
        recordOrchestratorService.describeAllRecordTypes(collectionId, "v0.2");

    Map<String, Integer> actualCounts =
        allTypes.stream()
            .collect(
                Collectors.toMap(
                    recordTypeSchema -> recordTypeSchema.name().getName(),
                    RecordTypeSchema::count));

    Map<String, Integer> expectedCounts =
        new ImmutableMap.Builder<String, Integer>()
            .put("data_release", 3)
            .put("submitted_aligned_reads", 1)
            .build();

    assertEquals(expectedCounts, actualCounts);
  }

  /* import precision.avro, and spot-check a record to ensure the numeric values inside that
       record preserved all of its decimal places.
  */
  @Test
  void numberPrecision() throws IOException, JobExecutionException {
    testSupport.executePfbImportQuartzJob(collectionId, testPrecisionResource);

    // this record, within the precision.avro file, is known to have numbers with high decimal
    // precision. Retrieve some of them and check for the expected high-precision values.
    RecordResponse recordResponse =
        recordOrchestratorService.getSingleRecord(
            collectionId, "v0.2", RecordType.valueOf("aliquot"), "aliquot_melituria_Khattish");

    // the expected values here match what is output by PyPFB
    assertEquals(
        BigDecimal.valueOf(76.98304748535156),
        recordResponse.recordAttributes().getAttributeValue("a260_a280_ratio"));
    assertEquals(
        BigDecimal.valueOf(24.167686462402344),
        recordResponse.recordAttributes().getAttributeValue("aliquot_quantity"));
    assertEquals(
        BigDecimal.valueOf(12.1218843460083),
        recordResponse.recordAttributes().getAttributeValue("concentration"));
  }

  @Test
  void importWithForwardRelations() throws IOException, JobExecutionException {
    // The first record in this file relates to the fourth;
    testSupport.executePfbImportQuartzJob(collectionId, forwardRelationsAvroResource);

    /* the forwardRelationsAvroResource should insert:
       - 1 record of type submitted_aligned_reads
       - 3 record(s) of type data_release, one of which relates to the submitted_aligned_reads record
    */

    RecordTypeSchema dataReleaseSchema =
        recordOrchestratorService.describeRecordType(
            collectionId, "v0.2", RecordType.valueOf("data_release"));

    assert (dataReleaseSchema
        .attributes()
        .contains(
            new AttributeSchema(
                "submitted_aligned_reads",
                DataTypeMapping.RELATION.toString(),
                RecordType.valueOf("submitted_aligned_reads"))));
    RecordResponse relatedRecord =
        recordOrchestratorService.getSingleRecord(
            collectionId,
            "v0.2",
            RecordType.valueOf("data_release"),
            "data_release.4622cdbf-9836-64a2-c743-e17b0708cbb6.2");

    assertEquals(
        RelationUtils.createRelationString(
            RecordType.valueOf("submitted_aligned_reads"), "HG01102_cram"),
        relatedRecord.recordAttributes().getAttributeValue("submitted_aligned_reads"));
  }

  @Test
  void importCyclicalRelations() throws IOException, JobExecutionException {
    // submitted_aligned_reads relates to data_release and vice versa
    testSupport.executePfbImportQuartzJob(collectionId, cyclicalAvroResource);

    RecordTypeSchema dataReleaseSchema =
        recordOrchestratorService.describeRecordType(
            collectionId, "v0.2", RecordType.valueOf("data_release"));

    assert (dataReleaseSchema
        .attributes()
        .contains(
            new AttributeSchema(
                "submitted_aligned_reads",
                DataTypeMapping.RELATION.toString(),
                RecordType.valueOf("submitted_aligned_reads"))));
    RecordResponse relatedRecord =
        recordOrchestratorService.getSingleRecord(
            collectionId,
            "v0.2",
            RecordType.valueOf("data_release"),
            "data_release.4622cdbf-9836-64a2-c743-e17b0708cbb6.2");

    assertEquals(
        RelationUtils.createRelationString(
            RecordType.valueOf("submitted_aligned_reads"), "HG01102_cram"),
        relatedRecord.recordAttributes().getAttributeValue("submitted_aligned_reads"));

    RecordTypeSchema alignedReadsSchema =
        recordOrchestratorService.describeRecordType(
            collectionId, "v0.2", RecordType.valueOf("submitted_aligned_reads"));

    assert (alignedReadsSchema
        .attributes()
        .contains(
            new AttributeSchema(
                "data_release",
                DataTypeMapping.RELATION.toString(),
                RecordType.valueOf("data_release"))));
    RecordResponse relatedRecord2 =
        recordOrchestratorService.getSingleRecord(
            collectionId, "v0.2", RecordType.valueOf("submitted_aligned_reads"), "HG01102_cram");

    assertEquals(
        RelationUtils.createRelationString(
            RecordType.valueOf("data_release"),
            "data_release.4622cdbf-9836-64a2-c743-e17b0708cbb6.2"),
        relatedRecord2.recordAttributes().getAttributeValue("data_release"));
  }

  /* import four_rows.avro, and validate the tables and row counts it imported. */
  @Test
  void noRequestsForUserEmail() throws IOException, JobExecutionException {
    testSupport.executePfbImportQuartzJob(collectionId, fourRowsAvroResource);

    // IN THE CONTROL PLANE, importing a PFB requires publishing to pubsub, which requires a call to
    // Sam to get the user's email. Here in the data plane, we should NOT ever make that request
    // to Sam, since we don't need it.
    verify(samDao, never()).getUserEmail(any());
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
