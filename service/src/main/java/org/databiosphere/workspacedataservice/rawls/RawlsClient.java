package org.databiosphere.workspacedataservice.rawls;

import static org.databiosphere.workspacedataservice.retry.RestClientRetry.RestCall;

import bio.terra.workspace.model.DataRepoSnapshotResource;
import java.util.UUID;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.springframework.web.client.RestClientResponseException;

/** Client to make REST calls to Rawls */
public class RawlsClient {
  private final RawlsApi rawlsApi;
  private final RestClientRetry restClientRetry;

  public RawlsClient(RawlsApi rawlsApi, RestClientRetry restClientRetry) {
    this.rawlsApi = rawlsApi;
    this.restClientRetry = restClientRetry;
  }

  public RawlsWorkspaceDetails getWorkspaceDetails(UUID workspaceId) {
    try {
      RestCall<RawlsWorkspaceDetails> restCall =
          () ->
              rawlsApi.getWorkspaceDetails(
                  workspaceId,
                  /* RawlsWorkspaceDetails only includes the subset of workspace fields needed by CWDS. */
                  /* fields */ String.join(",", RawlsWorkspaceDetails.SUPPORTED_FIELDS));

      return restClientRetry.withRetryAndErrorHandling(restCall, "Rawls.getWorkspaceDetails");
    } catch (RestClientResponseException restException) {
      throw new RawlsException(restException);
    }
  }

  public SnapshotListResponse enumerateDataRepoSnapshotReferences(
      UUID workspaceId, int offset, int limit) {
    try {
      RestCall<SnapshotListResponse> restCall =
          () -> rawlsApi.enumerateDataRepoSnapshotByWorkspaceId(workspaceId, offset, limit);

      return restClientRetry.withRetryAndErrorHandling(
          restCall, "Rawls.enumerateDataRepoSnapshotReferences");
    } catch (RestClientResponseException restException) {
      throw new RawlsException(restException);
    }
  }

  public void createSnapshotReference(UUID workspaceId, UUID snapshotId) {
    try {
      NamedDataRepoSnapshot namedDataRepoSnapshot = NamedDataRepoSnapshot.forSnapshotId(snapshotId);

      RestCall<DataRepoSnapshotResource> restCall =
          () -> rawlsApi.createDataRepoSnapshotByWorkspaceId(workspaceId, namedDataRepoSnapshot);

      // note we do not return the DataRepoSnapshotResource from this method
      restClientRetry.withRetryAndErrorHandling(restCall, "Rawls.createSnapshotReference");
    } catch (RestClientResponseException restException) {
      throw new RawlsException(restException);
    }
  }
}
