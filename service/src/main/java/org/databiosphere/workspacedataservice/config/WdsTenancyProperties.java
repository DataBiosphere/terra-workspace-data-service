package org.databiosphere.workspacedataservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "twds.tenancy")
public class WdsTenancyProperties {
  private Boolean allowVirtualCollections;
  private Boolean requireEnvWorkspace;

  public Boolean getAllowVirtualCollections() {
    return allowVirtualCollections;
  }

  public void setAllowVirtualCollections(Boolean allowVirtualCollections) {
    this.allowVirtualCollections = allowVirtualCollections;
  }

  public Boolean getRequireEnvWorkspace() {
    return requireEnvWorkspace;
  }

  public void setRequireEnvWorkspace(Boolean requireEnvWorkspace) {
    this.requireEnvWorkspace = requireEnvWorkspace;
  }
}
