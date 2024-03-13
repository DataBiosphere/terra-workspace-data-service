package org.databiosphere.workspacedataservice.rawls;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.model.DataRepoSnapshotResource;
import bio.terra.workspace.model.ResourceList;
import java.util.Objects;
import java.util.UUID;
import org.databiosphere.workspacedataservice.sam.TokenContextUtil;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

@Component
public class RawlsClient {

  private final String rawlsUrl;
  private final RestTemplate restTemplate;

  private static final Logger LOGGER = LoggerFactory.getLogger(RawlsClient.class);

  @Autowired
  public RawlsClient(String rawlsUrl, RestTemplate restTemplate) {
    this.rawlsUrl = rawlsUrl;
    this.restTemplate = restTemplate;
  }

  public ResourceList enumerateDataRepoSnapshotReferences(UUID workspaceId, int offset, int limit) {
    try {
      UriComponentsBuilder builder =
          UriComponentsBuilder.fromHttpUrl(getSnapshotsUrl(workspaceId))
              .queryParam("offset", offset)
              .queryParam("limit", limit);
      LOGGER.debug(
          "Listing snapshot references for workspace {} at URL {}",
          workspaceId,
          getSnapshotsUrl(workspaceId));

      ResponseEntity<ResourceList> response =
          restTemplate.exchange(
              builder.toUriString(),
              HttpMethod.GET,
              new HttpEntity<>(getAuthedHeaders()),
              ResourceList.class);
      // TODO proper error handling
      if (!response.getStatusCode().is2xxSuccessful()) {
        LOGGER.warn(
            "Unsuccessful response retrieving snapshot references for workspace {}", workspaceId);
      }
      return response.getBody();
    } catch (Exception e) {
      LOGGER.warn("Error retrieving snapshot references", e);
      throw e;
    }
  }

  public void createSnapshotReference(UUID workspaceId, UUID snapshotId) {
    try {
      ResponseEntity<DataRepoSnapshotResource> response =
          restTemplate.exchange(
              getSnapshotsUrl(workspaceId),
              HttpMethod.POST,
              new HttpEntity<>(new SnapshotModel().id(snapshotId), getAuthedHeaders()),
              DataRepoSnapshotResource.class);
      // TODO proper error handling

      if (!response.getStatusCode().is2xxSuccessful()) {
        LOGGER.warn(
            "Unsuccessful response creating snapshot reference {} for workspace {}",
            snapshotId,
            workspaceId);
      }
    } catch (Exception e) {
      LOGGER.warn("Error creating snapshot reference", e);
      throw e;
    }
  }

  private HttpHeaders getAuthedHeaders() {
    // TODO will we need other headers
    HttpHeaders headers = new HttpHeaders();
    BearerToken token = TokenContextUtil.getToken();

    // add the user's bearer token to the client
    if (token.nonEmpty()) {
      LOGGER.debug("setting access token for rawls request");
      headers.setBearerAuth(Objects.requireNonNull(token.getValue()));
    } else {
      LOGGER.warn("No access token found for rawls request.");
    }
    return headers;
  }

  private String getSnapshotsUrl(UUID workspaceId) {
    return rawlsUrl + "/api/workspaces/" + workspaceId + "/snapshots/v2";
  }
}
