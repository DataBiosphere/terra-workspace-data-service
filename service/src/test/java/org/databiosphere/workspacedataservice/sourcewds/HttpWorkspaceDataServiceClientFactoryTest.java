package org.databiosphere.workspacedataservice.sourcewds;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Set;
import org.databiosphere.workspacedata.client.ApiClient;
import org.databiosphere.workspacedata.client.auth.Authentication;
import org.databiosphere.workspacedata.client.auth.HttpBearerAuth;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class HttpWorkspaceDataServiceClientFactoryTest extends DataPlaneTestBase {

  @Autowired private WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory;

  @Test
  void bearerTokenIsSet() {
    String dummyToken = "dummy-auth-token";
    String dummyUrl = "https://example.com/";

    HttpWorkspaceDataServiceClientFactory httpWorkspaceDataServiceClientFactory =
        assertInstanceOf(
            HttpWorkspaceDataServiceClientFactory.class, workspaceDataServiceClientFactory);

    ApiClient apiClient = httpWorkspaceDataServiceClientFactory.getApiClient(dummyToken, dummyUrl);

    var authentications = apiClient.getAuthentications();
    assertEquals(Set.of("bearerAuth"), authentications.keySet());

    Authentication bearerAuth = apiClient.getAuthentication("bearerAuth");
    HttpBearerAuth httpBearerAuth = assertInstanceOf(HttpBearerAuth.class, bearerAuth);

    assertEquals(dummyToken, httpBearerAuth.getBearerToken(), "actual bearer token was incorrect");
  }
}
