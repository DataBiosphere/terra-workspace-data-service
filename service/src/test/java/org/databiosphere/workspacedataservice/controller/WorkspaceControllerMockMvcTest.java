package org.databiosphere.workspacedataservice.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.databiosphere.workspacedataservice.dao.SqlUtils.quote;
import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.JobTypeEnum.*;
import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum.*;
import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.CollectionServerModel;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.sam.MockSamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDaoFactory;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.stubbing.OngoingStubbing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MvcResult;

@DirtiesContext
// this class explicitly tests multi-tenancy, so it ensures that
// `enforce-collections-match-workspace-id` is turned off
@TestPropertySource(properties = {"twds.tenancy.enforce-collections-match-workspace-id=false"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkspaceControllerMockMvcTest extends MockMvcTestBase {

  @Autowired private CollectionService collectionService;
  @Autowired private JobDao jobDao;
  @Autowired private NamedParameterJdbcTemplate namedTemplate;
  @Autowired private ObjectMapper objectMapper;

  @MockitoBean SamAuthorizationDaoFactory samAuthorizationDaoFactory;

  private final SamAuthorizationDao samAuthorizationDao = spy(MockSamAuthorizationDao.allowAll());

  // delete all collections, across all workspaces
  private void cleanupAll() {
    List<UUID> collIds =
        namedTemplate.queryForList(
            "select distinct id from sys_wds.collection", Map.of(), UUID.class);
    collIds.forEach(
        collId -> {
          CollectionId collectionId = CollectionId.of(collId);
          namedTemplate
              .getJdbcTemplate()
              .update("drop schema " + quote(collectionId.toString()) + " cascade");
        });
    namedTemplate.getJdbcTemplate().update("delete from sys_wds.collection");
  }

  @BeforeEach
  void beforeEach() {
    cleanupAll();
  }

  @AfterAll
  void afterAll() {
    cleanupAll();
  }

  @Test
  void initNewWorkspace() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // execute the expected-case test flow
    expectedCase(workspaceId);
  }

  @Test
  void initIsIdempotent() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // execute the expected-case test flow, save the job result
    expectedCase(workspaceId);

    // call init-workspace again
    String requestBody = "{}";
    initAndWaitForSuccess(workspaceId, requestBody);

    // if init-workspace is idempotent, we should still have only one collection: the default one
    List<CollectionServerModel> collectionsAfter = collectionService.list(workspaceId);
    CollectionServerModel expectedCollection = new CollectionServerModel("default", "default");
    expectedCollection.id(workspaceId.id());
    List<CollectionServerModel> expected = List.of(expectedCollection);
    assertEquals(expected, collectionsAfter);
  }

  // if the user has read but not write, they should hit an AuthorizationException
  @Test
  void readPermission() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // user has read permission but not write
    stubReadWorkspacePermission(workspaceId).thenReturn(true);
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);

    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/workspaces/v1/{workspaceId}", workspaceId)
                    .content("{}")
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isForbidden())
            .andReturn();

    assertInstanceOf(AuthorizationException.class, mvcResult.getResolvedException());
  }

  // if the user has neither read nor write, they should hit an AuthenticationMaskableException
  @Test
  void noPermission() throws Exception {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // user has no permission
    stubReadWorkspacePermission(workspaceId).thenReturn(false);
    stubWriteWorkspacePermission(workspaceId).thenReturn(false);

    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/workspaces/v1/{workspaceId}", workspaceId)
                    .content("{}")
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isNotFound())
            .andReturn();

    assertInstanceOf(AuthenticationMaskableException.class, mvcResult.getResolvedException());
  }

  // helper: implementation of the expected-case test
  private GenericJobServerModel expectedCase(WorkspaceId workspaceId) throws Exception {
    // ensure write permission
    stubWriteWorkspacePermission(workspaceId).thenReturn(true);

    // before initialization, should have an empty list of collections
    List<CollectionServerModel> collectionsBefore = collectionService.list(workspaceId);
    assertThat(collectionsBefore).isEmpty();

    // call initWorkspace with a non-clone request
    String requestBody = "{}";
    GenericJobServerModel actualJob = initAndWaitForSuccess(workspaceId, requestBody);

    // after initialization, should have created a collection of the given id
    List<CollectionServerModel> collectionsAfter = collectionService.list(workspaceId);
    CollectionServerModel expectedCollection = new CollectionServerModel("default", "default");
    expectedCollection.id(workspaceId.id());
    List<CollectionServerModel> expected = List.of(expectedCollection);
    assertEquals(expected, collectionsAfter);

    // verify job result
    assertEquals(SUCCEEDED, actualJob.getStatus());
    assertEquals(WORKSPACE_INIT, actualJob.getJobType());
    assertNotNull(actualJob.getJobId());

    return actualJob;
  }

  // helper: call the init-workspace API, get a job id back, wait for that job to succeed.
  private GenericJobServerModel initAndWaitForSuccess(WorkspaceId workspaceId, String requestBody)
      throws Exception {
    // calling the API should result in 202 Accepted
    MvcResult mvcResult =
        mockMvc
            .perform(
                post("/workspaces/v1/{workspaceId}", workspaceId)
                    .content(requestBody)
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isAccepted())
            .andReturn();

    // get the job result, which gives us the job id
    GenericJobServerModel job =
        objectMapper.readValue(
            mvcResult.getResponse().getContentAsString(), GenericJobServerModel.class);
    await()
        .atMost(Duration.ofSeconds(30))
        .failFast(
            () -> {
              // fail fast if the job hits a fatal problem
              GenericJobServerModel currentJob = jobDao.getJob(job.getJobId());
              return Set.of(ERROR, CANCELLED, UNKNOWN).contains(currentJob.getStatus());
            })
        .until(
            () -> {
              // else, wait until the job succeeds
              GenericJobServerModel currentJob = jobDao.getJob(job.getJobId());
              return SUCCEEDED.equals(currentJob.getStatus());
            });

    // get the job once more so we can return it
    return jobDao.getJob(job.getJobId());
  }

  private OngoingStubbing<Boolean> stubReadWorkspacePermission(WorkspaceId workspaceId) {
    when(samAuthorizationDaoFactory.getSamAuthorizationDao(workspaceId))
        .thenReturn(samAuthorizationDao);
    return when(samAuthorizationDao.hasReadWorkspacePermission());
  }

  private OngoingStubbing<Boolean> stubWriteWorkspacePermission(WorkspaceId workspaceId) {
    when(samAuthorizationDaoFactory.getSamAuthorizationDao(workspaceId))
        .thenReturn(samAuthorizationDao);
    return when(samAuthorizationDao.hasWriteWorkspacePermission());
  }
}
