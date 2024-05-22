package org.databiosphere.workspacedataservice.controller;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.JobTypeEnum;
import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;
import static org.databiosphere.workspacedataservice.shared.model.job.Job.newJob;
import static org.databiosphere.workspacedataservice.shared.model.job.JobType.DATA_IMPORT;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.databiosphere.workspacedata.model.ErrorResponse;
import org.databiosphere.workspacedata.model.ImportRequest.TypeEnum;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = {"mock-sam"})
@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class JobControllerTest extends TestBase {
  private static final String TEST_IMPORT_URI = "http://some/uri";

  @Autowired private NamedParameterJdbcTemplate namedTemplate;
  @Autowired private TestRestTemplate restTemplate;
  @Autowired private JobDao jobDao;
  @MockBean private CollectionDao collectionDao;

  private final CollectionId collectionId = CollectionId.of(randomUUID());
  private UUID jobId;

  @BeforeAll
  void beforeAll() {
    // push jobs into the db, one of different collectionId
    var testJob1 = newJob(collectionId, DATA_IMPORT, makePfbJobInput());
    var testJob2 = newJob(collectionId, DATA_IMPORT, makePfbJobInput());
    var testJob3 = newJob(CollectionId.of(randomUUID()), DATA_IMPORT, makePfbJobInput());

    // save one of the jobIds
    jobId = testJob1.getJobId();

    // insert 3 jobs of status "created"
    jobDao.createJob(testJob1);
    jobDao.createJob(testJob2);
    jobDao.createJob(testJob3);
  }

  @AfterAll
  void afterAll() {
    // cleanup: delete everything from the job table
    namedTemplate.getJdbcTemplate().update("delete from sys_wds.job;");
  }

  @ParameterizedTest(name = "Return all jobs with a querystring of {0}")
  @ValueSource(strings = {"", "?someOtherParam=whatever", "?statuses="})
  void instanceJobsReturnAll(String queryString) {
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    HttpHeaders headers = new HttpHeaders();
    ResponseEntity<List<GenericJobServerModel>> result =
        restTemplate.exchange(
            "/job/v1/instance/{instanceUuid}" + queryString,
            HttpMethod.GET,
            new HttpEntity<>(headers),
            new ParameterizedTypeReference<List<GenericJobServerModel>>() {},
            collectionId);
    List<GenericJobServerModel> jobList = result.getBody();
    assertNotNull(jobList);
    // 3 jobs inserted in beforeAll, only 2 for this instanceId
    assertEquals(2, jobList.size());

    for (GenericJobServerModel job : jobList) {
      assertEquals(StatusEnum.CREATED, job.getStatus());
      assertEquals(JobTypeEnum.DATA_IMPORT, job.getJobType());
      Map<String, String> map = assertInstanceOf(Map.class, job.getInput());
      assertThat(map).containsEntry("uri", TEST_IMPORT_URI);
      assertThat(map).containsEntry("importType", TypeEnum.PFB.toString());
    }
  }

  @Test
  void instanceJobsWithMultipleStatuses() {
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    assertDoesNotThrow(() -> jobDao.updateStatus(jobId, StatusEnum.CANCELLED));
    HttpHeaders headers = new HttpHeaders();
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

  @ParameterizedTest(name = "Return Bad Request with ?statuses={0}")
  @ValueSource(strings = {"xasdaf", "QUEUED,bad,RUNNING"})
  void instanceJobsWithEmpStatuses(String statusValues) {
    when(collectionDao.collectionSchemaExists(collectionId.id())).thenReturn(true);
    HttpHeaders headers = new HttpHeaders();
    ResponseEntity<ErrorResponse> result =
        restTemplate.exchange(
            "/job/v1/instance/{instanceUuid}?statuses={statusValues}",
            HttpMethod.GET,
            new HttpEntity<>(headers),
            ErrorResponse.class,
            collectionId,
            statusValues);
    assertEquals(HttpStatus.BAD_REQUEST, result.getStatusCode());
  }

  private static ImportJobInput makePfbJobInput() {
    try {
      return ImportJobInput.from(
          new ImportRequestServerModel(
              ImportRequestServerModel.TypeEnum.PFB, new URI(TEST_IMPORT_URI)));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
