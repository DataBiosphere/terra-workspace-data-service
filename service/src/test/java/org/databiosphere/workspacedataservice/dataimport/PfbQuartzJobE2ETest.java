package org.databiosphere.workspacedataservice.dataimport;

import static org.databiosphere.workspacedataservice.dataimport.PfbTestUtils.buildQuartzJob;
import static org.databiosphere.workspacedataservice.dataimport.PfbTestUtils.stubJobContext;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import bio.terra.workspace.model.ResourceList;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
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
import org.databiosphere.workspacedataservice.service.model.RecordTypeSchema;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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

@ActiveProfiles(profiles = "mock-sam")
@DirtiesContext
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PfbQuartzJobE2ETest {

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
  @Value("classpath:four_tables.avro")
  Resource fourTablesAvroResource;

  @Value("classpath:test.avro")
  Resource testAvroResource;

  @Value("classpath:precision.avro")
  Resource testPrecisionResource;

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

    // could assert on individual column data types to see if they are good

    Map<String, Integer> actualCounts =
        allTypes.stream()
            .collect(
                Collectors.toMap(
                    recordTypeSchema -> recordTypeSchema.name().getName(),
                    RecordTypeSchema::count));

    assertEquals(
        Map.of(
            "activities", 3202, "files", 3202, "donors", 3202, "biosamples", 3202, "datasets", 1),
        actualCounts);
  }

  /* import four_tables.avro, and validate the tables and row counts it imported. */
  @Test
  void importFourTablesResource() throws IOException, JobExecutionException {
    ImportRequestServerModel importRequest =
        new ImportRequestServerModel(
            ImportRequestServerModel.TypeEnum.PFB, fourTablesAvroResource.getURI());

    // because we have a mock scheduler dao, this won't trigger Quartz
    GenericJobServerModel genericJobServerModel =
        importService.createImport(instanceId, importRequest);

    UUID jobId = genericJobServerModel.getJobId();
    JobExecutionContext mockContext = stubJobContext(jobId, fourTablesAvroResource, instanceId);

    // WSM should report no snapshots already linked to this workspace
    when(wsmDao.enumerateDataRepoSnapshotReferences(any(), anyInt(), anyInt()))
        .thenReturn(new ResourceList());

    buildQuartzJob(jobDao, wsmDao, restClientRetry, batchWriteService, activityLogger)
        .execute(mockContext);

    /* the fourTablesAvroResource should insert:
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

    assertEquals(Map.of("data_release", 3, "submitted_aligned_reads", 1), actualCounts);
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
}
