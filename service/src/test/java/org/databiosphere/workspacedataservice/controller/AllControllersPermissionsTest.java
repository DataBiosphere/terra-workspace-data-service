package org.databiosphere.workspacedataservice.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

import java.time.OffsetDateTime;
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
import org.databiosphere.workspacedataservice.service.RecordOrchestratorService;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

@DirtiesContext
@SpringBootTest
@ActiveProfiles({"mock-backup-dao", "mock-restore-dao", "mock-clone-dao"})
class AllControllersPermissionsTest extends MockMvcTestBase {
  @Autowired PermissionService permissionService;
  @MockBean SamAuthorizationDaoFactory samAuthorizationDaoFactory;
  @MockBean CollectionService collectionService;
  @MockBean RecordDao recordDao;
  @MockBean JobService jobService;
  @MockBean BackupRestoreService backupRestoreService;
  @MockBean RecordOrchestratorService recordOrchestratorService;
  @Autowired TwdsProperties twdsProperties;

  private final SamAuthorizationDao samAuthorizationDao = spy(MockSamAuthorizationDao.allowAll());

  // ========== exemplar data to ensure the internals of APIs succeed
  private static final WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
  private static final CollectionId collectionId = CollectionId.of(UUID.randomUUID());
  private static final UUID jobId = UUID.randomUUID();
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

  @BeforeEach
  void beforeEach() {
    // associate our exemplar collection with our exemplar workspace
    when(collectionService.getWorkspaceId(collectionId)).thenReturn(workspaceId);

    // the jobStatusV1 retrieves the job prior to the permission check (so it can determine which
    // collection the job belongs to). Thus, it needs a mock for its test to pass:
    when(jobService.getJob(jobId)).thenReturn(MOCK_JOB);
  }

  // ========== API definitions: which require read, which require write?

  // these APIs are public; they require no auth
  private static Stream<Arguments> publicApis() {
    // MockHttpServletRequestBuilder
    return Stream.of(
        arguments(named("GET /status", get("/status"))),
        arguments(named("GET /status/liveness", get("/status/liveness"))),
        arguments(named("GET /status/readiness", get("/status/readiness"))),
        arguments(named("GET /version", get("/version"))),
        arguments(named("GET /capabilities/v1", get("/capabilities/v1"))),
        arguments(named("GET /prometheus", get("/prometheus"))));
  }

  // these APIs require read permission
  private static Stream<Arguments> readOnlyApis() {
    return Stream.of(
        // Records
        arguments(
            named(
                "GET /{instanceid}/records/v0.2/{type}/{id}",
                get(
                    "/{instanceid}/records/v0.2/{type}/{id}",
                    collectionId,
                    RECORD_TYPE,
                    RECORD_ID))),
        arguments(
            named(
                "POST /{instanceid}/search/v0.2/{type}",
                post("/{instanceid}/search/v0.2/{type}", collectionId, RECORD_TYPE)
                    .content("{}")
                    .contentType(MediaType.APPLICATION_JSON))),
        arguments(
            named(
                "GET /{instanceid}/tsv/v0.2/{type}",
                get("/{instanceid}/tsv/v0.2/{type}", collectionId, RECORD_TYPE))),
        // Job
        arguments(named("GET /job/v1/{jobId}", get("/job/v1/{jobId}", jobId))),
        arguments(
            named(
                "GET /job/v1/instance/{instanceUuid}",
                get("/job/v1/instance/{instanceUuid}", collectionId))),
        // Collection
        arguments(
            named(
                "GET /collections/v1/{workspaceId}",
                get("/collections/v1/{workspaceId}", workspaceId))),
        arguments(
            named(
                "GET /collections/v1/{workspaceId}/{collectionId}",
                get("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))),
        // Schema
        arguments(
            named(
                "GET /{collectionId}/types/v0.2", get("/{collectionId}/types/v0.2", collectionId))),
        arguments(
            named(
                "GET /{collectionId}/types/v0.2/{type}",
                get("/{collectionId}/types/v0.2/{type}", collectionId, RECORD_TYPE))));
  }

  // these APIs require write permission
  private static Stream<Arguments> writeApis() {
    return Stream.of(
        // Records
        arguments(
            named(
                "POST /{instanceid}/batch/v0.2/{type}",
                post("/{instanceid}/batch/v0.2/{type}", collectionId, RECORD_TYPE)
                    .content("[]")
                    .contentType(MediaType.APPLICATION_JSON))),
        arguments(
            named(
                "PUT /{instanceid}/records/v0.2/{type}/{id}",
                put("/{instanceid}/records/v0.2/{type}/{id}", collectionId, RECORD_TYPE, RECORD_ID)
                    .content("{\"attributes\": {}}")
                    .contentType(MediaType.APPLICATION_JSON))),
        arguments(
            named(
                "PATCH /{instanceid}/records/v0.2/{type}/{id}",
                patch(
                        "/{instanceid}/records/v0.2/{type}/{id}",
                        collectionId,
                        RECORD_TYPE,
                        RECORD_ID)
                    .content("{\"attributes\": {}}")
                    .contentType(MediaType.APPLICATION_JSON))),
        arguments(
            named(
                "DELETE /{instanceid}/records/v0.2/{type}/{id}",
                delete(
                    "/{instanceid}/records/v0.2/{type}/{id}",
                    collectionId,
                    RECORD_TYPE,
                    RECORD_ID))),
        arguments(
            named(
                "POST /{instanceid}/tsv/v0.2/{type}",
                multipart("/{instanceid}/tsv/v0.2/{type}", collectionId, RECORD_TYPE)
                    .file(
                        new MockMultipartFile(
                            "records",
                            "tsv_orig.tsv",
                            MediaType.TEXT_PLAIN_VALUE,
                            "col1\tcol2\nfoo\tbar\n".getBytes())))),
        // Import
        arguments(
            named(
                "POST /{instanceUuid}/import/v1",
                post("/{instanceUuid}/import/v1", collectionId)
                    .content("{\"type\":\"PFB\",\"url\":\"https://example.com/something\"}")
                    .contentType(MediaType.APPLICATION_JSON))),
        // Collection
        arguments(
            named(
                "POST /collections/v1/{workspaceId}",
                post("/collections/v1/{workspaceId}", workspaceId, collectionId)
                    .content("{\"name\":\"foo\",\"description\":\"bar\"}")
                    .contentType(MediaType.APPLICATION_JSON))),
        arguments(
            named(
                "DELETE /collections/v1/{workspaceId}/{collectionId}",
                delete("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId))),
        arguments(
            named(
                "PUT /collections/v1/{workspaceId}/{collectionId}",
                put("/collections/v1/{workspaceId}/{collectionId}", workspaceId, collectionId)
                    .content("{\"name\":\"foo\",\"description\":\"bar\"}")
                    .contentType(MediaType.APPLICATION_JSON))),
        // Schema
        arguments(
            named(
                "DELETE /{instanceid}/types/v0.2/{type}",
                delete("/{instanceid}/types/v0.2/{type}", collectionId, RECORD_TYPE))),
        arguments(
            named(
                "PATCH /{instanceid}/types/v0.2/{type}/{attribute}",
                patch(
                        "/{instanceid}/types/v0.2/{type}/{attribute}",
                        collectionId,
                        RECORD_TYPE,
                        "myattribute")
                    .content("{\"name\":\"foo\",\"datatype\":\"BOOLEAN\"}")
                    .contentType(MediaType.APPLICATION_JSON))),
        arguments(
            named(
                "DELETE /{instanceid}/types/v0.2/{type}/{attribute}",
                delete(
                    "/{instanceid}/types/v0.2/{type}/{attribute}",
                    collectionId,
                    RECORD_TYPE,
                    "myattribute"))));
  }

  // these single-tenant APIs require read permission. When single-tenant APIs are fully retired,
  // this method and any tests that use it should be deleted.
  private static Stream<Arguments> singleTenantReadOnlyApis() {
    return Stream.of(
        // Cloning
        arguments(
            named(
                "POST /backup/v0.2",
                post("/backup/v0.2")
                    .content(
                        "{\"requestingWorkspaceId\": \"3fa85f64-5717-4562-b3fc-2c963f66afa6\","
                            + "  \"description\": \"string\"}")
                    .contentType(MediaType.APPLICATION_JSON))),
        arguments(
            named(
                "GET /backup/v0.2/{trackingId}",
                get("/backup/v0.2/{trackingId}", UUID.randomUUID()))),
        arguments(named("GET /clone/v0.2", get("/clone/v0.2"))));
  }

  // ========== write API tests

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

  // ========== read-only API tests

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

  // ========== single-tenant read-only API tests

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

  // ========== public API tests

  @ParameterizedTest(name = "public API {0} succeeds with no permission")
  @MethodSource("publicApis")
  void publicApiNoPermission(MockHttpServletRequestBuilder request) throws Exception {
    userNoPermissions(workspaceId);
    requestShouldSucceed(request);
  }

  // ========== helpers

  // if an API requires write permission, but the user only has read, PermissionService will throw
  // an AuthorizationException.
  private void requestShouldBeForbidden(MockHttpServletRequestBuilder request) throws Exception {
    mockMvc
        .perform(request)
        .andExpect(
            result ->
                assertInstanceOf(AuthorizationException.class, result.getResolvedException()));
  }

  // if the user has neither read nor write, PermissionService will throw
  // AuthenticationMaskableException
  private void requestShouldBeMaskedNotFound(MockHttpServletRequestBuilder request)
      throws Exception {
    mockMvc
        .perform(request)
        .andExpect(
            result ->
                assertInstanceOf(
                    AuthenticationMaskableException.class, result.getResolvedException()));
  }

  /* We don't bother mocking out all the internals of each API within this test class.
     Therefore, it is quite likely that APIs will fail for business logic reasons. All we
     care about for this test class is that the permission checks pass. We consider a passing
     test to be either:
      - no exception was thrown by the API
      - an exception was thrown, but that exception is neither AuthenticationMaskableException
          nor AuthorizationException; these are the two exceptions that would be thrown by
          permission checks.
  */
  private void requestShouldSucceed(MockHttpServletRequestBuilder request) throws Exception {
    mockMvc
        .perform(request)
        .andDo(
            mvcResult -> {
              if (mvcResult.getResolvedException() != null) {
                Exception ex = mvcResult.getResolvedException();
                assertThat(ex).isNotInstanceOf(AuthenticationMaskableException.class);
                assertThat(ex).isNotInstanceOf(AuthorizationException.class);
              }
            });
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
