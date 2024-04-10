package org.databiosphere.workspacedataservice.pact;

import static org.databiosphere.workspacedataservice.TestTags.PACT_TEST;
import static org.junit.jupiter.api.Assertions.*;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
import au.com.dius.pact.consumer.dsl.PactDslJsonRootValue;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.RequestResponsePact;
import au.com.dius.pact.core.model.annotations.Pact;
import io.micrometer.observation.tck.TestObservationRegistry;
import java.util.Map;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.sam.*;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationException;
import org.databiosphere.workspacedataservice.service.model.exception.RestServerException;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@Tag(PACT_TEST)
@ExtendWith(PactConsumerTestExt.class)
class SamPactTest {

  static final String dummyResourceId = "92276398-fbe4-414a-9304-e7dcf18ac80e";

  @BeforeEach
  void setUp() {
    // Without this setup, the HttpClient throws a "No thread-bound request found" error
    MockHttpServletRequest request = new MockHttpServletRequest();
    // Set the mock request as the current request context
    RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
  }

  @Pact(consumer = "wds", provider = "sam")
  public RequestResponsePact statusApiPact(PactDslWithProvider builder) {
    return builder
        .given("Sam is ok")
        .uponReceiving("a status request")
        .path("/status")
        .method("GET")
        .willRespondWith()
        .status(200)
        .body("{\"ok\": true, \"systems\": {}}")
        .toPact();
  }

  @Pact(consumer = "wds", provider = "sam")
  public RequestResponsePact downStatusApiPact(PactDslWithProvider builder) {
    return builder
        .given("Sam is not ok")
        .uponReceiving("a status request")
        .path("/status")
        .method("GET")
        .willRespondWith()
        .status(500)
        .body("{\"ok\": false, \"systems\": {}}")
        .toPact();
  }

  @Pact(consumer = "wds", provider = "sam")
  public RequestResponsePact writeNoPermissionPact(PactDslWithProvider builder) {
    return builder
        .given("user does not have write permission", Map.of("dummyResourceId", dummyResourceId))
        .uponReceiving("a request for write permission on workspace")
        .pathFromProviderState(
            "/api/resources/v2/workspace/${dummyResourceId}/action/write",
            String.format("/api/resources/v2/workspace/%s/action/write", dummyResourceId))
        .method("GET")
        .willRespondWith()
        .status(200)
        .body("false")
        .toPact();
  }

  @Pact(consumer = "wds", provider = "sam")
  public RequestResponsePact writePermissionPact(PactDslWithProvider builder) {
    return builder
        .given("user has write permission", Map.of("dummyResourceId", dummyResourceId))
        .uponReceiving("a request for write permission on workspace")
        .pathFromProviderState(
            "/api/resources/v2/workspace/${dummyResourceId}/action/write",
            String.format("/api/resources/v2/workspace/%s/action/write", dummyResourceId))
        .method("GET")
        .willRespondWith()
        .status(200)
        .body("true")
        .toPact();
  }

  @Pact(consumer = "wds", provider = "sam")
  public RequestResponsePact readPermissionPact(PactDslWithProvider builder) {
    return builder
        .given("user has read permission", Map.of("dummyResourceId", dummyResourceId))
        .uponReceiving("a request for read permission on workspace")
        .pathFromProviderState(
            "/api/resources/v2/workspace/${dummyResourceId}/action/read",
            String.format("/api/resources/v2/workspace/%s/action/read", dummyResourceId))
        .method("GET")
        .willRespondWith()
        .status(200)
        .body("true")
        .toPact();
  }

  @Pact(consumer = "wds", provider = "sam")
  public RequestResponsePact readNoPermissionPact(PactDslWithProvider builder) {
    return builder
        .given("user does not have read permission", Map.of("dummyResourceId", dummyResourceId))
        .uponReceiving("a request for read permission on workspace")
        .pathFromProviderState(
            "/api/resources/v2/workspace/${dummyResourceId}/action/read",
            String.format("/api/resources/v2/workspace/%s/action/read", dummyResourceId))
        .method("GET")
        .willRespondWith()
        .status(200)
        .body("false")
        .toPact();
  }

  @Pact(consumer = "wds", provider = "sam")
  public RequestResponsePact userStatusPact(PactDslWithProvider builder) {
    var userResponseShape =
        new PactDslJsonBody()
            .stringType("userSubjectId")
            .stringType("userEmail")
            .booleanType("adminEnabled")
            .booleanType("enabled");
    return builder
        .given("user status info request with access token")
        .uponReceiving("a request for the user's status")
        .path("/register/user/v2/self/info")
        .method("GET")
        .headers("Authorization", "Bearer accessToken")
        .willRespondWith()
        .status(200)
        .body(userResponseShape)
        .toPact();
  }

  @Pact(consumer = "wds", provider = "sam")
  public RequestResponsePact noUserStatusPact(PactDslWithProvider builder) {
    return builder
        .given("user status info request without access token")
        .uponReceiving("a request for the user's status without a token")
        .path("/register/user/v2/self/info")
        .method("GET")
        .willRespondWith()
        .status(401)
        .toPact();
  }

  @Pact(consumer = "wds", provider = "sam")
  public RequestResponsePact petTokenPact(PactDslWithProvider builder) {
    PactDslJsonRootValue responseBody = PactDslJsonRootValue.stringType("aToken");

    return builder
        .given("user exists")
        .uponReceiving("a pet token request")
        .path("/api/google/v1/user/petServiceAccount/token")
        .method("POST")
        .body(
            "[\"https://www.googleapis.com/auth/userinfo.email\","
                + "  \"https://www.googleapis.com/auth/userinfo.profile\"]")
        .willRespondWith()
        .status(200)
        .body(responseBody)
        .toPact();
  }

  @Test
  @PactTestFor(pactMethod = "statusApiPact", pactVersion = PactSpecVersion.V3)
  void testSamServiceStatusCheck(MockServer mockServer) {
    SamDao samDao = getSamDao(mockServer);

    SystemStatus samStatus = samDao.getSystemStatus();
    assertTrue(samStatus.getOk());
  }

  @Test
  @PactTestFor(pactMethod = "downStatusApiPact", pactVersion = PactSpecVersion.V3)
  void testSamServiceDown(MockServer mockServer) {
    SamDao samDao = getSamDao(mockServer);
    assertThrows(RestServerException.class, samDao::getSystemStatus, "down Sam should throw 500");
  }

  @Test
  @PactTestFor(pactMethod = "userStatusPact", pactVersion = PactSpecVersion.V3)
  void testSamServiceUserStatusInfo(MockServer mockServer) {
    SamDao samDao = getSamDao(mockServer);
    String userId = samDao.getUserId(BearerToken.of("accessToken"));
    assertNotNull(userId);
  }

  @Test
  @PactTestFor(pactMethod = "noUserStatusPact", pactVersion = PactSpecVersion.V3)
  void testSamServiceNoUser(MockServer mockServer) {
    SamDao samDao = getSamDao(mockServer);
    BearerToken token = BearerToken.empty();
    assertThrows(
        AuthenticationException.class,
        () -> samDao.getUserId(token),
        "userId request without token should throw 401");
  }

  @Test
  @PactTestFor(pactMethod = "readNoPermissionPact", pactVersion = PactSpecVersion.V3)
  void testSamReadNoPermission(MockServer mockServer) {
    SamAuthorizationDao samAuthorizationDao =
        getSamAuthorizationDao(mockServer, dummyWorkspaceId());

    assertFalse(samAuthorizationDao.hasReadWorkspacePermission());
  }

  @Test
  @PactTestFor(pactMethod = "readPermissionPact", pactVersion = PactSpecVersion.V3)
  void testSamReadPermission(MockServer mockServer) {
    SamAuthorizationDao samAuthorizationDao =
        getSamAuthorizationDao(mockServer, dummyWorkspaceId());

    assertTrue(samAuthorizationDao.hasReadWorkspacePermission());
  }

  @Test
  @PactTestFor(pactMethod = "writeNoPermissionPact", pactVersion = PactSpecVersion.V3)
  void testSamWriteNoPermission(MockServer mockServer) {
    SamAuthorizationDao samAuthorizationDao =
        getSamAuthorizationDao(mockServer, dummyWorkspaceId());

    assertFalse(samAuthorizationDao.hasWriteWorkspacePermission());
  }

  @Test
  @PactTestFor(pactMethod = "writePermissionPact", pactVersion = PactSpecVersion.V3)
  void testSamWritePermission(MockServer mockServer) {
    SamAuthorizationDao samAuthorizationDao =
        getSamAuthorizationDao(mockServer, dummyWorkspaceId());

    assertTrue(samAuthorizationDao.hasWriteWorkspacePermission());
  }

  @Test
  @PactTestFor(pactMethod = "petTokenPact", pactVersion = PactSpecVersion.V3)
  void testPetToken(MockServer mockServer) {
    SamDao samDao = getSamDao(mockServer);
    String petToken = samDao.getPetToken();
    assertNotNull(petToken);
  }

  private SamAuthorizationDao getSamAuthorizationDao(
      MockServer mockServer, WorkspaceId workspaceId) {
    return samAuthorizationDaoFactory(mockServer).getSamAuthorizationDao(workspaceId);
  }

  private SamDao getSamDao(MockServer mockServer) {
    return new HttpSamDao(
        new HttpSamClientFactory(mockServer.getUrl()),
        new RestClientRetry(TestObservationRegistry.create()));
  }

  private SamAuthorizationDaoFactory samAuthorizationDaoFactory(MockServer mockServer) {
    return new SamAuthorizationDaoFactory(
        new HttpSamClientFactory(mockServer.getUrl()),
        new RestClientRetry(TestObservationRegistry.create()));
  }

  private WorkspaceId dummyWorkspaceId() {
    return WorkspaceId.fromString(dummyResourceId);
  }
}
