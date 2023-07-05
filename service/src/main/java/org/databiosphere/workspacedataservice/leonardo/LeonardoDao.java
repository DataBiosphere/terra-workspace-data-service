package org.databiosphere.workspacedataservice.leonardo;

import com.nimbusds.oauth2.sdk.util.StringUtils;
import org.broadinstitute.dsde.workbench.client.leonardo.ApiException;
import org.broadinstitute.dsde.workbench.client.leonardo.model.AppStatus;
import org.broadinstitute.dsde.workbench.client.leonardo.model.AppType;
import org.broadinstitute.dsde.workbench.client.leonardo.model.ListAppResponse;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
      // it is acceptable to fail if we cant find a single RUNNING wds in the proxy urls
      var url = extractWdsUrl(response);
      if (url != null) {
        return url;
      }

      throw new ApiException("Did not locate an app running WDS.");
    } catch (ApiException e) {
      throw new LeonardoServiceException(e);
    }
  }

  class AppCreationDateComparator implements Comparator<ListAppResponse> {
    @Override
    public int compare(ListAppResponse o1, ListAppResponse o2) {
      return o1.getAuditInfo().getCreatedDate().compareTo(o2.getAuditInfo().getCreatedDate());
    }
    @Override
    public boolean equals(Object obj) {
      return false;
    }

    @Override
    public int hashCode() {
      // sonar is complaining that hashCode has to be overwritten as well if equals is
      // since we are not using any object instances in this class, returning a default value 
      return -1;
    }
  }
  public String extractWdsUrl(List<ListAppResponse> response) {
    // look for apps of type "WDS" which are in "RUNNING" status.
    // if more than one, choose the earliest-created. This matches Terra UI logic.
    Optional<ListAppResponse> maybeApp = response.stream()
            .filter(app -> AppType.WDS.equals(app.getAppType()) && AppStatus.RUNNING.equals(app.getStatus()))
            .min(new AppCreationDateComparator());
    if (maybeApp.isPresent()) {
      Map<String, String> proxyUrls = ((Map<String, String>) maybeApp.get().getProxyUrls());
      if (proxyUrls != null) {
        String url = proxyUrls.getOrDefault("wds", "");
        if (StringUtils.isNotBlank(url)) {
          return url;
        }
      }
    }
    return null;
  }
}