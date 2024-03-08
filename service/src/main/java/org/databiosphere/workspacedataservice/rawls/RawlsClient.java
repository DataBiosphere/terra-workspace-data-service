package org.databiosphere.workspacedataservice.rawls;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class RawlsClient {

  private final String rawlsUrl;
  private final RestTemplate restTemplate;
  private final HttpHeaders headers;

  private static final Logger LOGGER = LoggerFactory.getLogger(RawlsClient.class);

  @Autowired
  public RawlsClient(String rawlsUrl, RestTemplate restTemplate) {
    this.rawlsUrl = rawlsUrl;
    this.restTemplate = restTemplate;
    this.headers = new HttpHeaders();
  }
  /*
  public WorkspaceResponse getWorkspace(UUID workspaceId, AuthenticatedUserRequest userRequest) {
    HttpHeaders authedHeaders = new HttpHeaders(headers);
    authedHeaders.setBearerAuth(userRequest.getToken());
    String userEmail = userRequest.getEmail();
    try {
      ResponseEntity<WorkspaceResponse> workspaceCall =
          restTemplate.exchange(
              getWorkspaceEndpoint(workspaceId),
              HttpMethod.GET,
              new HttpEntity<>(headers),
              WorkspaceResponse.class);
      if (!workspaceCall.getStatusCode().is2xxSuccessful()) {
        logger.warn("Unsuccessful response retrieving workspace {} by {}", workspaceId, userEmail);
      }
      return workspaceCall.getBody();
    } catch (Exception e) {
      logger.warn("Error retrieving workspace", e);
      throw e;
    }
  }*/
}
