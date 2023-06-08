package org.databiosphere.workspacedataservice.leonardo;

import org.broadinstitute.dsde.workbench.client.leonardo.ApiException;
import org.broadinstitute.dsde.workbench.client.leonardo.model.ListAppResponse;

import java.util.List;

public class LeonardoDao {
  private final LeonardoClientFactory leonardoClientFactory;
  private final String workspaceId;

  public LeonardoDao(LeonardoClientFactory leonardoClientFactory, String workspaceId) {
    this.leonardoClientFactory = leonardoClientFactory;
    this.workspaceId = workspaceId;
  }

  /**
   * Asks leo to return apps running in a given workspace setting.
   */
  public String getWdsEndpointUrl(String token) {
    var workspaceApps = this.leonardoClientFactory.getAppsV2Api(token);
    try {
      var response = workspaceApps.listAppsV2(workspaceId, null, false, null);

      return "";
    } catch (ApiException e) {
      throw new LeonardoServiceException(e);
    }
  }

  private String findUrlForWds(List<ListAppResponse> apps)  {
    return "";
  }
}