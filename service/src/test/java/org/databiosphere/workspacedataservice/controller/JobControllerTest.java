package org.databiosphere.workspacedataservice.controller;

import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.JobTypeEnum;
import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;
import static org.databiosphere.workspacedataservice.shared.model.job.Job.newJob;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.config.InstanceProperties.SingleTenant;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dao.WorkspaceIdDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;
import org.databiosphere.workspacedataservice.shared.model.job.JobType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"mock-sam"})
@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class JobControllerTest extends TestBase {
  @Autowired private NamedParameterJdbcTemplate namedTemplate;
  @Autowired private TestRestTemplate restTemplate;
  @Autowired private JobDao jobDao;
  @Autowired @SingleTenant private WorkspaceId workspaceId;

  @MockBean private CollectionDao collectionDao;
  @MockBean private WorkspaceIdDao workspaceIdDao;

  private CollectionId collectionId;

  private UUID jobId;

  @BeforeEach
  void setup() {
    clearJobs();
    collectionId = CollectionId.of(workspaceId.id());
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    when(workspaceIdDao.getWorkspaceId(eq(collectionId))).thenReturn(workspaceId);

    // push jobs into the db, one of different collectionId
    Job<JobInput, JobResult> testJob1 = newJob(collectionId, JobType.DATA_IMPORT, JobInput.empty());
    Job<JobInput, JobResult> testJob2 = newJob(collectionId, JobType.DATA_IMPORT, JobInput.empty());
    Job<JobInput, JobResult> testJob3 =
        newJob(CollectionId.of(UUID.randomUUID()), JobType.DATA_IMPORT, JobInput.empty());

    // save one of the jobIds
    jobId = testJob1.getJobId();

    // insert 2 jobs of status "created"
    jobDao.createJob(testJob1);
    jobDao.createJob(testJob2);
    jobDao.createJob(testJob3);
  }

  @AfterEach
  void clearJobs() {
    // cleanup: delete everything from the job table
    namedTemplate.getJdbcTemplate().update("delete from sys_wds.job;");
  }

  @Test
  void instanceJobsReturnAllNoStatus() {
    HttpHeaders headers = new HttpHeaders();
    ResponseEntity<List<GenericJobServerModel>> result =
        restTemplate.exchange(
            "/job/v1/instance/{instanceUuid}?statuses={statuses}",
            HttpMethod.GET,
            new HttpEntity<>(headers),
            new ParameterizedTypeReference<>() {},
            collectionId,
            "");
    List<GenericJobServerModel> jobList = result.getBody();
    assertNotNull(jobList);
    // 3 jobs inserted in beforeAll, only 2 for this instanceId
    assertEquals(2, jobList.size());
    assertEquals(StatusEnum.CREATED, jobList.get(0).getStatus());
    assertEquals(JobTypeEnum.DATA_IMPORT, jobList.get(0).getJobType());
  }

  @Test
  void instanceJobsWithMultipleStatuses() {
    assertDoesNotThrow(() -> jobDao.updateStatus(jobId, StatusEnum.CANCELLED));
    HttpHeaders headers = new HttpHeaders();
    ResponseEntity<List<GenericJobServerModel>> result =
        restTemplate.exchange(
            "/job/v1/instance/{instanceUuid}?statuses={status1}&statuses={status2}",
            HttpMethod.GET,
            new HttpEntity<>(headers),
            new ParameterizedTypeReference<>() {},
            collectionId,
            "CREATED",
            "CANCELLED");
    List<GenericJobServerModel> jobList = result.getBody();
    assertNotNull(jobList);
    // 3 jobs inserted in beforeAll, only 2 for this instanceId
    assertEquals(2, jobList.size());
    assertEquals(StatusEnum.CREATED, jobList.get(0).getStatus());
    assertEquals(StatusEnum.CANCELLED, jobList.get(1).getStatus());
  }
}
