package org.databiosphere.workspacedataservice.pact;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.RequestResponsePact;
import au.com.dius.pact.core.model.annotations.Pact;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.databiosphere.workspacedataservice.sam.*;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationException;
import org.databiosphere.workspacedataservice.service.model.exception.SamServerException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Tag("pact-test")
@ExtendWith(PactConsumerTestExt.class)
class SamPactTest {

    static final String dummyResourceId = "92276398-fbe4-414a-9304-e7dcf18ac80e";

    @BeforeAll
    static void setup() {
        //Without this setup, the HttpClient throws a "No thread-bound request found" error
        MockHttpServletRequest request = new MockHttpServletRequest();
        // Set the mock request as the current request context
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
    }

    @Pact(consumer = "wds-consumer", provider = "sam-provider")
    public RequestResponsePact statusApiPact(PactDslWithProvider builder) {
        return builder
                .given("Sam is ok")
                .uponReceiving("a status request")
                .path("/status")
                .method("GET")
                .willRespondWith()
                .status(200)
                .body("{\"ok\": true}")
                .toPact();
    }

    @Pact(consumer = "wds-consumer", provider = "sam-provider")
    public RequestResponsePact downStatusApiPact(PactDslWithProvider builder) {
        return builder
                .given("Sam is not ok")
                .uponReceiving("a status request")
                .path("/status")
                .method("GET")
                .willRespondWith()
                .status(500)
                .body("{\"ok\": false}")
                .toPact();
    }

    @Pact(consumer = "wds-consumer", provider = "sam-provider")
    public RequestResponsePact writeNoPermissionPact(PactDslWithProvider builder) {
        return builder
                .given("user does not have write permission", Map.of("dummyResourceId",
                        dummyResourceId))
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

    @Pact(consumer = "wds-consumer", provider = "sam-provider")
    public RequestResponsePact writePermissionPact(PactDslWithProvider builder) {
        return builder
                .given("user has write permission", Map.of("dummyResourceId",
                                dummyResourceId))
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

    @Pact(consumer = "wds-consumer", provider = "sam-provider")
    public RequestResponsePact deletePermissionPact(PactDslWithProvider builder) {
        return builder
                .given("user has delete permission", Map.of("dummyResourceId",
                        dummyResourceId))
                .uponReceiving("a request for delete permission on workspace")
                .pathFromProviderState(
                        "/api/resources/v2/workspace/${dummyResourceId}/action/delete",
                        String.format("/api/resources/v2/workspace/%s/action/delete", dummyResourceId))
                .method("GET")
                .willRespondWith()
                .status(200)
                .body("true")
                .toPact();
    }

    @Pact(consumer = "wds-consumer", provider = "sam-provider")
    public RequestResponsePact deleteNoPermissionPact(PactDslWithProvider builder) {
        return builder
                .given("user does not have delete permission", Map.of("dummyResourceId",
                        dummyResourceId))
                .uponReceiving("a request for delete permission on workspace")
                .pathFromProviderState(
                        "/api/resources/v2/workspace/${dummyResourceId}/action/delete",
                        String.format("/api/resources/v2/workspace/%s/action/delete", dummyResourceId))
                .method("GET")
                .willRespondWith()
                .status(200)
                .body("false")
                .toPact();
    }

    @Pact(consumer = "wds-consumer", provider = "sam-provider")
    public RequestResponsePact userStatusPact(PactDslWithProvider builder) {
        var userResponseShape =
                new PactDslJsonBody()
                        .stringType("userSubjectId")
                        .stringType("userEmail")
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

    @Pact(consumer = "wds-consumer", provider = "sam-provider")
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

    @Test
    @PactTestFor(pactMethod = "statusApiPact", pactVersion = PactSpecVersion.V3)
    void testSamServiceStatusCheck(MockServer mockServer) {
        SamClientFactory clientFactory = new HttpSamClientFactory(mockServer.getUrl());
        SamDao samDao = new HttpSamDao(clientFactory, new HttpSamClientSupport(), UUID.randomUUID().toString());

        SystemStatus samStatus = samDao.getSystemStatus();
        assertTrue(samStatus.getOk());
    }

    @Test
    @PactTestFor(pactMethod = "downStatusApiPact", pactVersion = PactSpecVersion.V3)
    void testSamServiceDown(MockServer mockServer) {
        SamClientFactory clientFactory = new HttpSamClientFactory(mockServer.getUrl());
        SamDao samDao = new HttpSamDao(clientFactory, new HttpSamClientSupport(), UUID.randomUUID().toString());
        assertThrows(SamServerException.class,
                () -> samDao.getSystemStatus(),
                "down Sam should throw 500");
    }

    @Test
    @PactTestFor(pactMethod = "userStatusPact", pactVersion = PactSpecVersion.V3)
    void testSamServiceUserStatusInfo(MockServer mockServer) {
        SamClientFactory clientFactory = new HttpSamClientFactory(mockServer.getUrl());
        SamDao samDao = new HttpSamDao(clientFactory, new HttpSamClientSupport(), UUID.randomUUID().toString());
        String userId = samDao.getUserId("accessToken");
        assertNotNull(userId);
    }

    @Test
    @PactTestFor(pactMethod = "noUserStatusPact", pactVersion = PactSpecVersion.V3)
    void testSamServiceNoUser(MockServer mockServer) {
        SamClientFactory clientFactory = new HttpSamClientFactory(mockServer.getUrl());
        SamDao samDao = new HttpSamDao(clientFactory, new HttpSamClientSupport(), UUID.randomUUID().toString());
        assertThrows(AuthenticationException.class,
                () -> samDao.getUserId(null),
                "userId request without token should throw 401");
    }

    @Test
    @PactTestFor(pactMethod = "deleteNoPermissionPact", pactVersion = PactSpecVersion.V3)
    void testSamDeleteNoPermission(MockServer mockServer) {
        SamClientFactory clientFactory = new HttpSamClientFactory(mockServer.getUrl());
        SamDao samDao = new HttpSamDao(clientFactory, new HttpSamClientSupport(), dummyResourceId);

        assertFalse(samDao.hasDeleteInstancePermission());
    }

    @Test
    @PactTestFor(pactMethod = "deletePermissionPact", pactVersion = PactSpecVersion.V3)
    void testSamDeletePermission(MockServer mockServer) {
        SamClientFactory clientFactory = new HttpSamClientFactory(mockServer.getUrl());
        SamDao samDao = new HttpSamDao(clientFactory, new HttpSamClientSupport(), dummyResourceId);

        assertTrue(samDao.hasDeleteInstancePermission());
    }

    @Test
    @PactTestFor(pactMethod = "writeNoPermissionPact", pactVersion = PactSpecVersion.V3)
    void testSamWriteNoPermission(MockServer mockServer) {
        SamClientFactory clientFactory = new HttpSamClientFactory(mockServer.getUrl());
        SamDao samDao = new HttpSamDao(clientFactory, new HttpSamClientSupport(), dummyResourceId);

        assertFalse(samDao.hasWriteInstancePermission());
    }

    @Test
    @PactTestFor(pactMethod = "writePermissionPact", pactVersion = PactSpecVersion.V3)
    void testSamWritePermission(MockServer mockServer) {
        SamClientFactory clientFactory = new HttpSamClientFactory(mockServer.getUrl());
        SamDao samDao = new HttpSamDao(clientFactory, new HttpSamClientSupport(), dummyResourceId);

        assertTrue(samDao.hasWriteInstancePermission());
    }

}
