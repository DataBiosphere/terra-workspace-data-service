package org.databiosphere.workspacedataservice.rawls;

import static org.databiosphere.workspacedataservice.retry.RestClientRetry.RestCall;

import bio.terra.workspace.model.DataRepoSnapshotResource;
import java.util.UUID;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClientResponseException;

/** Client to make REST calls to Rawls */
public class RawlsClient {
  private final RawlsApi rawlsApi;
  private final RestClientRetry restClientRetry;

  private static final Logger LOGGER = LoggerFactory.getLogger(RawlsClient.class);

  public RawlsClient(RawlsApi rawlsApi, RestClientRetry restClientRetry) {
    this.rawlsApi = rawlsApi;
    this.restClientRetry = restClientRetry;
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

  // TODO: (AJ-1705) Add cloning instructions COPY_REFERENCE and a purpose=policy
  //     key-value pair to the reference’s properties
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
