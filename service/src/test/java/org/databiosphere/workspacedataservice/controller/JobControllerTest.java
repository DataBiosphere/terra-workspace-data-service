package org.databiosphere.workspacedataservice.controller;

import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.JobTypeEnum;
import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;
import static org.databiosphere.workspacedataservice.shared.model.job.Job.newJob;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;
import org.databiosphere.workspacedataservice.shared.model.job.JobType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

  @MockBean private CollectionDao collectionDao;

  private final CollectionId collectionId = CollectionId.of(UUID.randomUUID());

  private UUID jobId;

  @BeforeAll
  void beforeAll() {
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

  @AfterAll
  void afterAll() {
    // cleanup: delete everything from the job table
    namedTemplate.getJdbcTemplate().update("delete from sys_wds.job;");
  }

  @Test
  void instanceJobsReturnAll() {
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    HttpHeaders headers = new HttpHeaders();
    ResponseEntity<List<GenericJobServerModel>> result =
        restTemplate.exchange(
            "/job/v1/instance/{instanceUuid}",
            HttpMethod.GET,
            new HttpEntity<>(headers),
            new ParameterizedTypeReference<List<GenericJobServerModel>>() {},
            collectionId);
    List<GenericJobServerModel> jobList = result.getBody();
    assertNotNull(jobList);
    // 3 jobs inserted in beforeAll, only 2 for this instanceId
    assertEquals(2, jobList.size());
    assertEquals(StatusEnum.CREATED, jobList.get(0).getStatus());
    assertEquals(JobTypeEnum.DATA_IMPORT, jobList.get(0).getJobType());
  }

  @Test
  void instanceJobsWithMultipleStatuses() {
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    assertDoesNotThrow(() -> jobDao.updateStatus(jobId, StatusEnum.CANCELLED));
    HttpHeaders headers = new HttpHeaders();
    // ParameterizedTypeReference<List<GenericJobServerModel>> returnType = new
    // ParameterizedTypeReference<List<GenericJobServerModel>>() {};
    ResponseEntity<List<GenericJobServerModel>> result =
        restTemplate.exchange(
            "/job/v1/instance/{instanceUuid}?statuses={status1}&statuses={status2}",
            HttpMethod.GET,
            new HttpEntity<>(headers),
            new ParameterizedTypeReference<List<GenericJobServerModel>>() {},
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

  @Test
  void instanceJobsWithMultipleDelimitedStatuses() {
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    assertDoesNotThrow(() -> jobDao.updateStatus(jobId, StatusEnum.CANCELLED));
    HttpHeaders headers = new HttpHeaders();
    // ParameterizedTypeReference<List<GenericJobServerModel>> returnType = new
    // ParameterizedTypeReference<List<GenericJobServerModel>>() {};
    ResponseEntity<List<GenericJobServerModel>> result =
        restTemplate.exchange(
            "/job/v1/instance/{instanceUuid}?statuses={status1},{status2}",
            HttpMethod.GET,
            new HttpEntity<>(headers),
            new ParameterizedTypeReference<List<GenericJobServerModel>>() {},
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
