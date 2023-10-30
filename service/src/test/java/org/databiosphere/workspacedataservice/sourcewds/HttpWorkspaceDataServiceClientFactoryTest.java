package org.databiosphere.workspacedataservice.sourcewds;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Set;
import org.databiosphere.workspacedata.client.ApiClient;
import org.databiosphere.workspacedata.client.auth.Authentication;
import org.databiosphere.workspacedata.client.auth.HttpBearerAuth;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class HttpWorkspaceDataServiceClientFactoryTest {

  @Autowired private WorkspaceDataServiceClientFactory workspaceDataServiceClientFactory;

  @Test
  void bearerTokenIsSet() {
    String dummyToken = "dummy-auth-token";
    String dummyUrl = "https://example.com/";

    if (workspaceDataServiceClientFactory
        instanceof HttpWorkspaceDataServiceClientFactory httpWorkspaceDataServiceClientFactory) {
      ApiClient apiClient =
          httpWorkspaceDataServiceClientFactory.getApiClient(dummyToken, dummyUrl);

      var authentications = apiClient.getAuthentications();
      assertEquals(Set.of("bearerAuth"), authentications.keySet());

      Authentication bearerAuth = apiClient.getAuthentication("bearerAuth");
      if (bearerAuth instanceof HttpBearerAuth httpBearerAuth) {
        assertEquals(
            dummyToken, httpBearerAuth.getBearerToken(), "actual bearer token was incorrect");
      } else {
        fail("bearerAuth was wrong class: " + bearerAuth.getClass().getName());
      }
    } else {
      fail(
          "workspaceDataServiceClientFactory was wrong class: "
              + workspaceDataServiceClientFactory.getClass().getName());
    }
  }
}
