package org.databiosphere.workspacedataservice.dataimport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.TestTags.SLOW;
import static org.databiosphere.workspacedataservice.dataimport.PfbTestUtils.buildQuartzJob;
import static org.databiosphere.workspacedataservice.dataimport.PfbTestUtils.stubJobContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
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
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dao.SchedulerDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.service.BatchWriteService;
import org.databiosphere.workspacedataservice.service.ImportService;
import org.databiosphere.workspacedataservice.service.InstanceService;
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.service.RelationUtils;
import org.databiosphere.workspacedataservice.service.model.AttributeSchema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.Resource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

/**
 * Tests for PFB import that execute "end-to-end" - that is, they go through the whole process of
 * parsing the PFB, creating tables in Postgres, inserting rows, and then reading back the
 * rows/counts/schema from Postgres.
 */
@ActiveProfiles(profiles = "mock-sam")
@DirtiesContext
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PfbQuartzJobE2ETest {

  @Autowired JobDao jobDao;
  @Autowired RestClientRetry restClientRetry;
  @Autowired BatchWriteService batchWriteService;
  @Autowired ActivityLogger activityLogger;
  @Autowired RecordOrchestratorService recordOrchestratorService;
  @Autowired ImportService importService;
  @Autowired InstanceService instanceService;

  @MockBean SchedulerDao schedulerDao;
  @MockBean WorkspaceManagerDao wsmDao;

  // test resources used below
  @Value("classpath:four_rows.avro")
  Resource fourRowsAvroResource;

  @Value("classpath:test.avro")
  Resource testAvroResource;

  @Value("classpath:precision.avro")
  Resource testPrecisionResource;

  @Value("classpath:forward_relations.avro")
  Resource forwardRelationsAvroResource;

  @Value("classpath:cyclical.pfb")
  Resource cyclicalAvroResource;

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

  /* import test.avro, and validate the tables and row counts it imported. */
  @Test
  @Tag(SLOW)
  void importTestResource() throws IOException, JobExecutionException {
    ImportRequestServerModel importRequest =
        new ImportRequestServerModel(
            ImportRequestServerModel.TypeEnum.PFB, testAvroResource.getURI());

    // because we have a mock scheduler dao, this won't trigger Quartz
    GenericJobServerModel genericJobServerModel =
        importService.createImport(instanceId, importRequest);

    UUID jobId = genericJobServerModel.getJobId();
    JobExecutionContext mockContext = stubJobContext(jobId, testAvroResource, instanceId);

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());

    buildQuartzJob(jobDao, wsmDao, restClientRetry, batchWriteService, activityLogger)
        .execute(mockContext);

    /* the testAvroResource should insert:
       - 3202 record(s) of type activities
       - 3202 record(s) of type files
       - 3202 record(s) of type donors
       - 3202 record(s) of type biosamples
       - 1 record(s) of type datasets
    */

    List<RecordTypeSchema> allTypes =
        recordOrchestratorService.describeAllRecordTypes(instanceId, "v0.2");

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
    ImportRequestServerModel importRequest =
        new ImportRequestServerModel(
            ImportRequestServerModel.TypeEnum.PFB, fourRowsAvroResource.getURI());

    // because we have a mock scheduler dao, this won't trigger Quartz
    GenericJobServerModel genericJobServerModel =
        importService.createImport(instanceId, importRequest);

    UUID jobId = genericJobServerModel.getJobId();
    JobExecutionContext mockContext = stubJobContext(jobId, fourRowsAvroResource, instanceId);

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());

    buildQuartzJob(jobDao, wsmDao, restClientRetry, batchWriteService, activityLogger)
        .execute(mockContext);

    /* the fourRowsAvroResource should insert:
       - 3 record(s) of type data_release
       - 1 record(s) of type submitted_aligned_reads
    */

    List<RecordTypeSchema> allTypes =
        recordOrchestratorService.describeAllRecordTypes(instanceId, "v0.2");

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
    ImportRequestServerModel importRequest =
        new ImportRequestServerModel(
            ImportRequestServerModel.TypeEnum.PFB, testPrecisionResource.getURI());

    // because we have a mock scheduler dao, this won't trigger Quartz
    GenericJobServerModel genericJobServerModel =
        importService.createImport(instanceId, importRequest);

    UUID jobId = genericJobServerModel.getJobId();
    JobExecutionContext mockContext = stubJobContext(jobId, testPrecisionResource, instanceId);

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());

    buildQuartzJob(jobDao, wsmDao, restClientRetry, batchWriteService, activityLogger)
        .execute(mockContext);

    // this record, within the precision.avro file, is known to have numbers with high decimal
    // precision. Retrieve some of them and check for the expected high-precision values.
    RecordResponse recordResponse =
        recordOrchestratorService.getSingleRecord(
            instanceId, "v0.2", RecordType.valueOf("aliquot"), "aliquot_melituria_Khattish");

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

  // TODO this file is very similar to the fourRowsResource; should we combine this test with that
  // one?
  @Test
  void importWithForwardRelations() throws IOException, JobExecutionException {
    // The first record in this file relates to the fourth;
    // TODO do we want to set batch size to 2 so that forward relation is in a later batch?
    ImportRequestServerModel importRequest =
        new ImportRequestServerModel(
            ImportRequestServerModel.TypeEnum.PFB, forwardRelationsAvroResource.getURI());

    // because we have a mock scheduler dao, this won't trigger Quartz
    GenericJobServerModel genericJobServerModel =
        importService.createImport(instanceId, importRequest);

    UUID jobId = genericJobServerModel.getJobId();
    JobExecutionContext mockContext =
        stubJobContext(jobId, forwardRelationsAvroResource, instanceId);

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());

    buildQuartzJob(jobDao, wsmDao, restClientRetry, batchWriteService, activityLogger)
        .execute(mockContext);

    /* the forwardRelationsAvroResource should insert:
       - 1 record of type submitted_aligned_reads
       - 3 record(s) of type data_release, one of which relates to the submitted_aligned_reads record
    */

    RecordTypeSchema dataReleaseSchema =
        recordOrchestratorService.describeRecordType(
            instanceId, "v0.2", RecordType.valueOf("data_release"));

    assert (dataReleaseSchema
        .attributes()
        .contains(
            new AttributeSchema(
                "submitted_aligned_reads",
                DataTypeMapping.RELATION.toString(),
                RecordType.valueOf("submitted_aligned_reads"))));
    RecordResponse relatedRecord =
        recordOrchestratorService.getSingleRecord(
            instanceId,
            "v0.2",
            RecordType.valueOf("data_release"),
            "data_release.4622cdbf-9836-64a2-c743-e17b0708cbb6.2");

    assertEquals(
        RelationUtils.createRelationString(
            RecordType.valueOf("submitted_aligned_reads"), "HG01102_cram"),
        relatedRecord.recordAttributes().getAttributeValue("submitted_aligned_reads"));
  }

  // TODO this cyclical file also includes a forward relation; do we need both of these tests?
  @Test
  void importCyclicalRelations() throws IOException, JobExecutionException {
    // submitted_aligned_reads relates to data_release and vice versa
    ImportRequestServerModel importRequest =
        new ImportRequestServerModel(
            ImportRequestServerModel.TypeEnum.PFB, cyclicalAvroResource.getURI());

    // because we have a mock scheduler dao, this won't trigger Quartz
    GenericJobServerModel genericJobServerModel =
        importService.createImport(instanceId, importRequest);

    UUID jobId = genericJobServerModel.getJobId();
    JobExecutionContext mockContext = stubJobContext(jobId, cyclicalAvroResource, instanceId);

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());

    buildQuartzJob(jobDao, wsmDao, restClientRetry, batchWriteService, activityLogger)
        .execute(mockContext);

    RecordTypeSchema dataReleaseSchema =
        recordOrchestratorService.describeRecordType(
            instanceId, "v0.2", RecordType.valueOf("data_release"));

    assert (dataReleaseSchema
        .attributes()
        .contains(
            new AttributeSchema(
                "submitted_aligned_reads",
                DataTypeMapping.RELATION.toString(),
                RecordType.valueOf("submitted_aligned_reads"))));
    RecordResponse relatedRecord =
        recordOrchestratorService.getSingleRecord(
            instanceId,
            "v0.2",
            RecordType.valueOf("data_release"),
            "data_release.4622cdbf-9836-64a2-c743-e17b0708cbb6.2");

    assertEquals(
        RelationUtils.createRelationString(
            RecordType.valueOf("submitted_aligned_reads"), "HG01102_cram"),
        relatedRecord.recordAttributes().getAttributeValue("submitted_aligned_reads"));

    RecordTypeSchema alignedReadsSchema =
        recordOrchestratorService.describeRecordType(
            instanceId, "v0.2", RecordType.valueOf("submitted_aligned_reads"));

    assert (alignedReadsSchema
        .attributes()
        .contains(
            new AttributeSchema(
                "data_release",
                DataTypeMapping.RELATION.toString(),
                RecordType.valueOf("data_release"))));
    RecordResponse relatedRecord2 =
        recordOrchestratorService.getSingleRecord(
            instanceId, "v0.2", RecordType.valueOf("submitted_aligned_reads"), "HG01102_cram");

    assertEquals(
        RelationUtils.createRelationString(
            RecordType.valueOf("data_release"),
            "data_release.4622cdbf-9836-64a2-c743-e17b0708cbb6.2"),
        relatedRecord2.recordAttributes().getAttributeValue("data_release"));
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
