package org.databiosphere.workspacedataservice.rawls;

import static org.databiosphere.workspacedataservice.annotations.DeploymentMode.*;

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
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

@Component
@ControlPlane
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

      ResponseEntity<ResourceList> response =
          restTemplate.exchange(
              builder.toUriString(),
              HttpMethod.GET,
              new HttpEntity<>(getAuthedHeaders()),
              ResourceList.class);
      return response.getBody();
    } catch (RestClientResponseException e) {
      LOGGER.warn("Error retrieving snapshot references for workspace {}", workspaceId, e);
      throw new RawlsException(e);
    }
  }

  // TODO: (AJ-1705) Add cloning instructions COPY_REFERENCE and a purpose=policy
  // key-value pair to the reference’s properties
  public void createSnapshotReference(UUID workspaceId, UUID snapshotId) {
    try {
      restTemplate.exchange(
          getSnapshotsUrl(workspaceId),
          HttpMethod.POST,
          new HttpEntity<>(new SnapshotModel().id(snapshotId), getAuthedHeaders()),
          DataRepoSnapshotResource.class);
    } catch (RestClientResponseException e) {
      LOGGER.warn(
          "Error creating snapshot reference for snapshotId {} in workspace {}",
          snapshotId,
          workspaceId,
          e);
      throw new RawlsException(e);
    }
  }

  // Get the user's token from the context and attach it to headers
  private HttpHeaders getAuthedHeaders() {
    HttpHeaders headers = new HttpHeaders();
    BearerToken token = TokenContextUtil.getToken();

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
