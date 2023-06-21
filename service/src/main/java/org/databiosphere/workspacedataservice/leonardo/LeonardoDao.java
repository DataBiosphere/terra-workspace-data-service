package org.databiosphere.workspacedataservice.leonardo;

import org.broadinstitute.dsde.workbench.client.leonardo.ApiException;

import java.util.Map;

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
      // unsure what the key would be if there is more than 1 wds present in the listed apps, but in this case our assumption is
      // it is acceptable to fail if we cant find a single wds in the proxy urls
      for(int i=0; i< response.size(); i++) {
        Map<String, String> proxyUrls = ((Map<String, String>) response.get(i).getProxyUrls());
        var url = proxyUrls.get("wds");
        if(url != null) {
          return url;
        }
      }
      throw new ApiException("Did not locate WDS source url.");
    } catch (ApiException e) {
      throw new LeonardoServiceException(e);
    }
  }
}