package org.databiosphere.workspacedataservice.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.shared.model.job.JobType.SYNC_BACKUP;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.config.TwdsProperties;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.sam.MockSamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDao;
import org.databiosphere.workspacedataservice.sam.SamAuthorizationDaoFactory;
import org.databiosphere.workspacedataservice.service.BackupRestoreService;
import org.databiosphere.workspacedataservice.service.CollectionService;
import org.databiosphere.workspacedataservice.service.JobService;
import org.databiosphere.workspacedataservice.service.PermissionService;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.shared.model.BackupResponse;
import org.databiosphere.workspacedataservice.shared.model.BackupRestoreRequest;
import org.databiosphere.workspacedataservice.shared.model.CloneResponse;
import org.databiosphere.workspacedataservice.shared.model.CloneStatus;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

@DirtiesContext
@SpringBootTest
@ActiveProfiles({"mock-collection-dao", "mock-backup-dao", "mock-restore-dao", "mock-clone-dao"})
class AllControllersPermissionsTest extends MockMvcTestBase {
  @Autowired PermissionService permissionService;
  @MockBean SamAuthorizationDaoFactory samAuthorizationDaoFactory;
  @MockBean CollectionService collectionService;
  @MockBean RecordDao recordDao;
  @MockBean JobService jobService;
  @MockBean BackupRestoreService backupRestoreService;
  @Autowired TwdsProperties twdsProperties;

  private final SamAuthorizationDao samAuthorizationDao = spy(MockSamAuthorizationDao.allowAll());

  // ========== exemplar data to ensure the internals of APIs succeed
  private static final WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
  private static final CollectionId collectionId = CollectionId.of(UUID.randomUUID());
  private static final UUID jobId = UUID.randomUUID();
  private static final UUID backupTrackingId = UUID.randomUUID();

  private static final RecordType RECORD_TYPE = RecordType.valueOf("mytype");
  private static final String RECORD_ID = "myid";

  private static final GenericJobServerModel MOCK_JOB =
      new GenericJobServerModel(
          jobId,
          GenericJobServerModel.JobTypeEnum.DATA_IMPORT,
          collectionId.id(),
          GenericJobServerModel.StatusEnum.RUNNING,
          OffsetDateTime.now(),
          OffsetDateTime.now());

  private static final Record MOCK_RECORD = new Record(RECORD_ID, RECORD_TYPE);

  private static final BackupResponse MOCK_BACKUPRESPONSE =
      new BackupResponse("filename", UUID.randomUUID(), "description");

  private static final CloneResponse MOCK_CLONERESPONSE =
      new CloneResponse(UUID.randomUUID(), CloneStatus.BACKUPSUCCEEDED);

  private static final Job<JobInput, BackupResponse> MOCK_BACKUPJOB =
      new Job<>(
          UUID.randomUUID(),
          SYNC_BACKUP,
          /* collectionId= */ null, // backup jobs do not execute within a single collection
          JobStatus.QUEUED,
          "",
          Timestamp.from(Instant.now()).toLocalDateTime(),
          Timestamp.from(Instant.now()).toLocalDateTime(),
          JobInput.empty(),
          MOCK_BACKUPRESPONSE);

  private static final Job<JobInput, CloneResponse> MOCK_CLONEJOB =
      new Job<>(
          UUID.randomUUID(),
          SYNC_BACKUP,
          /* collectionId= */ null, // backup jobs do not execute within a single collection
          JobStatus.QUEUED,
          "",
          Timestamp.from(Instant.now()).toLocalDateTime(),
          Timestamp.from(Instant.now()).toLocalDateTime(),
          JobInput.empty(),
          MOCK_CLONERESPONSE);

  @BeforeEach
  void beforeEach() {
    // mocks so APIs don't hit any internal errors
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    when(recordDao.recordTypeExists(collectionId.id(), RECORD_TYPE)).thenReturn(true);
    when(recordDao.getSingleRecord(eq(collectionId.id()), eq(RECORD_TYPE), anyString()))
        .thenReturn(Optional.of(MOCK_RECORD));
    when(recordDao.getPrimaryKeyColumn(RECORD_TYPE, collectionId.id())).thenReturn("sys_name");

    when(jobService.getJob(jobId)).thenReturn(MOCK_JOB);

    when(backupRestoreService.backupAzureWDS(
            eq("v0.2"), eq(backupTrackingId), any(BackupRestoreRequest.class)))
        .thenReturn(MOCK_BACKUPJOB);

    when(backupRestoreService.checkCloneStatus()).thenReturn(MOCK_CLONEJOB);
  }

  // ========== API definitions: which require read, which require write?

  // these APIs are public; they require no auth
  private static Stream<Arguments> publicApis() {
    // MockHttpServletRequestBuilder
    return Stream.of(
        arguments(named("GET /version", get("/version"))),
        arguments(named("GET /status", get("/status"))),
        arguments(named("GET /capabilities/v1", get("/capabilities/v1"))),
        arguments(named("GET /status/liveness", get("/status/liveness"))),
        arguments(named("GET /status/readiness", get("/status/readiness"))),
        arguments(named("GET /prometheus", get("/prometheus"))));
  }

  // these APIs require read permission
  private static Stream<Arguments> readOnlyApis() {
    return Stream.of(
        arguments(
            named(
                "GET /collections/v1/{workspaceId}",
                get("/collections/v1/{workspaceId}", workspaceId))),
        arguments(
            named(
                "GET /collections/v1/{workspaceId}/{collectionId}",
                get("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))),
        arguments(
            named(
                "GET /job/v1/instance/{collectionId}",
                get("/job/v1/instance/{collectionId}", collectionId))),
        arguments(named("GET /job/v1/{jobId}", get("/job/v1/{jobId}", jobId))),
        // get("/instances/v0.2"),
        arguments(
            named(
                "GET /{collectionId}/records/v0.2/{type}/{id}",
                get(
                    "/{collectionId}/records/v0.2/{type}/{id}",
                    collectionId,
                    RECORD_TYPE,
                    RECORD_ID))),
        arguments(
            named(
                "GET /{collectionId}/tsv/v0.2/{type}",
                get("/{collectionId}/tsv/v0.2/{type}", collectionId, RECORD_TYPE))),
        arguments(
            named(
                "GET /{collectionId}/types/v0.2", get("/{collectionId}/types/v0.2", collectionId))),
        arguments(
            named(
                "GET /{collectionId}/types/v0.2/{type}",
                get("/{collectionId}/types/v0.2/{type}", collectionId, RECORD_TYPE))),
        arguments(
            named(
                "POST /{collectionId}/search/v0.2/{type}",
                post("/{collectionId}/search/v0.2/{type}", collectionId, RECORD_TYPE)
                    .content("{}")
                    .contentType(MediaType.APPLICATION_JSON))));
  }

  // these APIs require write permission
  private static Stream<Arguments> writeApis() {
    return Stream.of(
        arguments(
            named(
                "PUT /{instanceid}/records/v0.2/{type}/{id}",
                put("/{instanceid}/records/v0.2/{type}/{id}", collectionId, RECORD_TYPE, RECORD_ID)
                    .content("{\"attributes\": {}}")
                    .contentType(MediaType.APPLICATION_JSON))));
    // TODO tests for these APIs:
    /*
     POST /{instanceid}/batch/{v}/{type}
     PATCH /{instanceid}/records/{v}/{type}/{id}
     DELETE /{instanceid}/records/{v}/{type}/{id}

     POST /{instanceid}/tsv/{v}/{type}

     POST /{instanceUuid}/import/v1

     POST /collections/v1/{workspaceId}

     PUT /collections/v1/{workspaceId}/{collectionId}
     DELETE /collections/v1/{workspaceId}/{collectionId}

     DELETE /instances/{v}/{instanceid}

     DELETE /{instanceid}/types/{v}/{type}
     PATCH /{instanceid}/types/{v}/{type}/{attribute}
     DELETE /{instanceid}/types/{v}/{type}/{attribute}
    */
  }

  // these single-tenant APIs require read permission. When single-tenant APIs are fully retired,
  // this method and any tests that use it should be deleted.
  private static Stream<Arguments> singleTenantReadOnlyApis() {
    return Stream.of(
        arguments(named("GET /instances/v0.2", get("/instances/v0.2"))),
        arguments(
            named(
                "POST /backup/v0.2",
                post("/backup/v0.2")
                    .content(
                        "{\"requestingWorkspaceId\": \"3fa85f64-5717-4562-b3fc-2c963f66afa6\","
                            + "  \"description\": \"string\"}")
                    .contentType(MediaType.APPLICATION_JSON))),
        arguments(named("GET /clone/v0.2", get("/clone/v0.2"))),
        arguments(
            named(
                "GET /backup/v0.2/{trackingId}",
                get("/backup/v0.2/{trackingId}", UUID.randomUUID()))));
  }

  // these single-tenant APIs require write permission. When single-tenant APIs are fully retired,
  // this method and any tests that use it should be deleted.
  private static Stream<Arguments> singleTenantWriteApis() {
    return Stream.of(
        arguments(
            named(
                "POST /instances/v0.2/{instanceid}",
                post("/instances/v0.2/{instanceid}", UUID.randomUUID()))));
  }

  // ========== write APIs

  @ParameterizedTest(name = "write API {0} succeeds with write permission")
  @MethodSource("writeApis")
  void writeApiWritePermission(MockHttpServletRequestBuilder request) throws Exception {
    userCanWrite(workspaceId);
    requestShouldSucceed(request);
  }

  @ParameterizedTest(name = "write API {0} is forbidden with read-only permission")
  @MethodSource("writeApis")
  void writeApiReadOnlyPermission(MockHttpServletRequestBuilder request) throws Exception {
    userCanRead(workspaceId);
    requestShouldBeForbidden(request);
  }

  @ParameterizedTest(name = "write API {0} is not found with no permission")
  @MethodSource("writeApis")
  void writeApiNoPermission(MockHttpServletRequestBuilder request) throws Exception {
    userNoPermissions(workspaceId);
    requestShouldBeMaskedNotFound(request);
  }

  // ========== read-only APIs

  @ParameterizedTest(name = "read-only API {0} succeeds with write permission")
  @MethodSource("readOnlyApis")
  void readApiWritePermission(MockHttpServletRequestBuilder request) throws Exception {
    userCanWrite(workspaceId);
    requestShouldSucceed(request);
  }

  @ParameterizedTest(name = "read-only API {0} succeeds with read-only permission")
  @MethodSource("readOnlyApis")
  void readApiReadOnlyPermission(MockHttpServletRequestBuilder request) throws Exception {
    userCanRead(workspaceId);
    requestShouldSucceed(request);
  }

  @ParameterizedTest(name = "read-only API {0} is not found with no permission")
  @MethodSource("readOnlyApis")
  void readApiNoPermission(MockHttpServletRequestBuilder request) throws Exception {
    userNoPermissions(workspaceId);
    requestShouldBeMaskedNotFound(request);
  }

  // ========== single-tenant write APIs

  @ParameterizedTest(name = "single-tenant write API {0} succeeds with write permission")
  @MethodSource("singleTenantWriteApis")
  void singleTenantWriteApiWritePermission(MockHttpServletRequestBuilder request) throws Exception {
    userCanWrite(twdsProperties.workspaceId());
    requestShouldSucceed(request);
  }

  @ParameterizedTest(name = "single-tenant write API {0} is forbidden with read-only permission")
  @MethodSource("singleTenantWriteApis")
  void singleTenantWriteApiReadOnlyPermission(MockHttpServletRequestBuilder request)
      throws Exception {
    userCanRead(twdsProperties.workspaceId());
    requestShouldBeForbidden(request);
  }

  @ParameterizedTest(name = "single-tenant write API {0} is not found with no permission")
  @MethodSource("singleTenantWriteApis")
  void singleTenantWriteApiNoPermission(MockHttpServletRequestBuilder request) throws Exception {
    userNoPermissions(twdsProperties.workspaceId());
    requestShouldBeMaskedNotFound(request);
  }

  // ========== single-tenant read-only APIs

  @ParameterizedTest(name = "single-tenant read-only API {0} succeeds with write permission")
  @MethodSource("singleTenantReadOnlyApis")
  void singleTenantReadApiWritePermission(MockHttpServletRequestBuilder request) throws Exception {
    userCanWrite(twdsProperties.workspaceId());
    requestShouldSucceed(request);
  }

  @ParameterizedTest(name = "single-tenant read-only API {0} succeeds with read-only permission")
  @MethodSource("singleTenantReadOnlyApis")
  void singleTenantReadApiReadOnlyPermission(MockHttpServletRequestBuilder request)
      throws Exception {
    userCanRead(twdsProperties.workspaceId());
    requestShouldSucceed(request);
  }

  @ParameterizedTest(name = "single-tenant read-only API {0} fails with no permission")
  @MethodSource("singleTenantReadOnlyApis")
  void singleTenantReadApiNoPermission(MockHttpServletRequestBuilder request) throws Exception {
    userNoPermissions(twdsProperties.workspaceId());
    requestShouldBeMaskedNotFound(request);
  }

  // ========== public APIs

  @ParameterizedTest(name = "public API {0} succeeds with no permission")
  @MethodSource("publicApis")
  void publicApiNoPermission(MockHttpServletRequestBuilder request) throws Exception {
    userNoPermissions(workspaceId);
    requestShouldSucceed(request);
  }

  // ========== helpers
  private void requestShouldSucceed(MockHttpServletRequestBuilder request) throws Exception {
    mockMvc.perform(request).andDo(this::assertSuccessfulPermissionCheck);
  }

  private void requestShouldBeForbidden(MockHttpServletRequestBuilder request) throws Exception {
    mockMvc
        .perform(request)
        .andExpect(
            result ->
                assertInstanceOf(AuthorizationException.class, result.getResolvedException()));
  }

  private void requestShouldBeMaskedNotFound(MockHttpServletRequestBuilder request)
      throws Exception {
    mockMvc
        .perform(request)
        .andExpect(
            result ->
                assertInstanceOf(
                    AuthenticationMaskableException.class, result.getResolvedException()));
  }

  private void assertSuccessfulPermissionCheck(MvcResult mvcResult) {
    /* We don't bother mocking out all the internals of each API within this test class.
       Therefore, it is quite likely that APIs will fail for business logic reasons. All we
       care about for this test class is that the permission checks pass. We consider a passing
       test to be either:
        - no exception was thrown, and response code is 2xx
        - an exception was thrown, but that exception is neither AuthenticationMaskableException
            nor AuthorizationException; these are the two exceptions that would be thrown by
            permission checks.
    */
    if (mvcResult.getResolvedException() == null) {
      assertThat(mvcResult.getResponse().getStatus() / 100).isEqualTo(2);
    } else {
      Exception ex = mvcResult.getResolvedException();
      assertThat(ex).isNotInstanceOf(AuthenticationMaskableException.class);
      assertThat(ex).isNotInstanceOf(AuthorizationException.class);
    }
  }

  private void userCanWrite(WorkspaceId workspaceId) {
    stubWorkspacePermissions(workspaceId, true, true);
  }

  private void userCanRead(WorkspaceId workspaceId) {
    stubWorkspacePermissions(workspaceId, true, false);
  }

  private void userNoPermissions(WorkspaceId workspaceId) {
    stubWorkspacePermissions(workspaceId, false, false);
  }

  private void stubWorkspacePermissions(
      WorkspaceId workspaceId, boolean canRead, boolean canWrite) {
    when(samAuthorizationDaoFactory.getSamAuthorizationDao(workspaceId))
        .thenReturn(samAuthorizationDao);
    when(samAuthorizationDao.hasReadWorkspacePermission()).thenReturn(canRead);
    when(samAuthorizationDao.hasWriteWorkspacePermission()).thenReturn(canWrite);
  }
}
