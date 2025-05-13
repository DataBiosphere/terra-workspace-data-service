package org.databiosphere.workspacedataservice.rawls;

import static org.databiosphere.workspacedataservice.retry.RestClientRetry.RestCall;
import static org.databiosphere.workspacedataservice.retry.RestClientRetry.VoidRestCall;

import bio.terra.workspace.model.DataRepoSnapshotResource;
import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.retry.RestClientRetry;
import org.springframework.web.client.RestClientResponseException;

/** Client to make REST calls to Rawls */
public class RawlsClient {
  /* RawlsWorkspaceDetails only includes the subset of workspace fields needed by CWDS.
   * Thus, RawlsClient should request only those fields when getting workspace details. */
  private final String GET_WORKSPACE_DETAILS_FIELDS_PARAM =
      String.join(",", RawlsWorkspaceDetails.SUPPORTED_FIELDS);
  private final RawlsApi rawlsApi;
  private final RestClientRetry restClientRetry;

  public RawlsClient(RawlsApi rawlsApi, RestClientRetry restClientRetry) {
    this.rawlsApi = rawlsApi;
    this.restClientRetry = restClientRetry;
  }

  public RawlsWorkspaceDetails getWorkspaceDetails(UUID workspaceId) {
    try {
      RestCall<RawlsWorkspaceDetails> restCall =
          () -> rawlsApi.getWorkspaceDetails(workspaceId, GET_WORKSPACE_DETAILS_FIELDS_PARAM);

      return restClientRetry.withRetryAndErrorHandling(restCall, "Rawls.getWorkspaceDetails");
    } catch (RestClientResponseException restException) {
      throw new RawlsException(restException);
    }
  }

  public void createSnapshotReferences(UUID workspaceId, List<UUID> snapshotIds) {
    try {
      RestCall<DataRepoSnapshotResource> restCall =
          () -> rawlsApi.createSnapshotsByWorkspaceIdV3(workspaceId, snapshotIds);

      // note we do not return the DataRepoSnapshotResource from this method
      restClientRetry.withRetryAndErrorHandling(restCall, "Rawls.createSnapshotReference");
    } catch (RestClientResponseException restException) {
      throw new RawlsException(restException);
    }
  }

  public void addAuthDomainGroups(
      String workspaceNamespace, String workspaceName, List<String> authDomainGroups) {
    try {
      VoidRestCall restCall =
          () -> rawlsApi.addAuthDomainGroups(workspaceNamespace, workspaceName, authDomainGroups);

      restClientRetry.withRetryAndErrorHandling(restCall, "Rawls.addAuthDomainGroups");
    } catch (RestClientResponseException restException) {
      throw new RawlsException(restException);
    }
  }
}
