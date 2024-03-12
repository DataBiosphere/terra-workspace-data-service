package org.databiosphere.workspacedataservice.rawls;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.model.DataRepoSnapshotResource;
import bio.terra.workspace.model.ResourceList;
import java.util.UUID;
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
  private final HttpHeaders headers;

  private static final Logger LOGGER = LoggerFactory.getLogger(RawlsClient.class);

  @Autowired
  public RawlsClient(String rawlsUrl, RestTemplate restTemplate) {
    this.rawlsUrl = rawlsUrl;
    this.restTemplate = restTemplate;
    this.headers = new HttpHeaders();
  }

  public ResourceList enumerateDataRepoSnapshotReferences(UUID workspaceId, int offset, int limit) {
    try {
      // TODO add auth headers
      UriComponentsBuilder builder =
          UriComponentsBuilder.fromHttpUrl(
                  rawlsUrl + "/api/workspaces/" + workspaceId + "/snapshots/v2")
              .queryParam("offset", offset)
              .queryParam("limit", limit);

      ResponseEntity<ResourceList> response =
          restTemplate.exchange(
              builder.toUriString(), HttpMethod.GET, new HttpEntity<>(headers), ResourceList.class);
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
    // TODO add auth headers
    try {
      ResponseEntity<DataRepoSnapshotResource> response =
          restTemplate.exchange(
              rawlsUrl + "/api/workspaces/" + workspaceId + "/snapshots/v2",
              HttpMethod.POST,
              new HttpEntity<>(new SnapshotModel().id(snapshotId), headers),
              DataRepoSnapshotResource.class);
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
}
