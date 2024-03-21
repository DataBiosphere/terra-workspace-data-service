package org.databiosphere.workspacedataservice.rawls;

import static org.databiosphere.workspacedataservice.retry.RestClientRetry.RestCall;

import bio.terra.workspace.model.DataRepoSnapshotResource;
import java.net.URI;
import java.util.Objects;
import java.util.UUID;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.databiosphere.workspacedataservice.sam.TokenContextUtil;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

/** Client to make REST calls to Rawls */
class RawlsClient {

  private final String rawlsUrl;
  // TODO: consider using RestClient instead of RestTemplate
  private final RestTemplate restTemplate;
  private final RestClientRetry restClientRetry;

  private static final Logger LOGGER = LoggerFactory.getLogger(RawlsClient.class);

  public RawlsClient(String rawlsUrl, RestTemplate restTemplate, RestClientRetry restClientRetry) {
    this.rawlsUrl = rawlsUrl;
    this.restTemplate = restTemplate;
    this.restClientRetry = restClientRetry;
  }

  public SnapshotListResponse enumerateDataRepoSnapshotReferences(
      UUID workspaceId, int offset, int limit) {
    try {
      URI targetUri =
          UriComponentsBuilder.fromHttpUrl(rawlsUrl)
              .pathSegment("api", "workspaces", workspaceId.toString(), "snapshots", "v2")
              .queryParam("offset", offset)
              .queryParam("limit", limit)
              .build()
              .toUri();

      HttpEntity<?> requestEntity = new HttpEntity<>(getAuthedHeaders());

      RestCall<ResponseEntity<SnapshotListResponse>> restCall =
          () ->
              restTemplate.exchange(
                  targetUri, HttpMethod.GET, requestEntity, SnapshotListResponse.class);

      ResponseEntity<SnapshotListResponse> response =
          restClientRetry.withRetryAndErrorHandling(
              restCall, "Rawls.enumerateDataRepoSnapshotReferences");

      return response.getBody();
    } catch (RestClientResponseException restException) {
      throw new RawlsException(restException);
    }
  }

  // TODO: (AJ-1705) Add cloning instructions COPY_REFERENCE and a purpose=policy
  // key-value pair to the reference’s properties
  public void createSnapshotReference(UUID workspaceId, UUID snapshotId) {
    try {
      URI targetUri =
          UriComponentsBuilder.fromHttpUrl(rawlsUrl)
              .pathSegment("api", "workspaces", workspaceId.toString(), "snapshots", "v2")
              .build()
              .toUri();

      HttpEntity<NamedDataRepoSnapshot> requestEntity =
          new HttpEntity<>(NamedDataRepoSnapshot.forSnapshotId(snapshotId), getAuthedHeaders());

      RestCall<ResponseEntity<DataRepoSnapshotResource>> restCall =
          () ->
              restTemplate.exchange(
                  targetUri, HttpMethod.POST, requestEntity, DataRepoSnapshotResource.class);

      // note we do not return the DataRepoSnapshotResource from this method
      restClientRetry.withRetryAndErrorHandling(restCall, "Rawls.createSnapshotReference");
    } catch (RestClientResponseException restException) {
      throw new RawlsException(restException);
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
}
