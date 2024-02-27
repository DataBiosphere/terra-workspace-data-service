package org.databiosphere.workspacedataservice.config;

/**
 * Configuration properties related to multi- or single-tenancy of workspaces within this WDS
 * deployment.
 */
public class TenancyProperties {
  private boolean allowVirtualCollections;
  private boolean requireEnvWorkspace;

  /**
   * Does this WDS deployment allow virtual collections? That is, does it allow for a collection
   * (f/k/a "instance") which does not have a corresponding row in the sys_wds.collection table? The
   * primary use case for virtual collections is when running as cWDS for imports only. Virtual
   * collections will always have collection id == workspace id.
   *
   * @return Does this WDS deployment allow virtual collections?
   */
  public boolean getAllowVirtualCollections() {
    return allowVirtualCollections;
  }

  void setAllowVirtualCollections(boolean allowVirtualCollections) {
    this.allowVirtualCollections = allowVirtualCollections;
  }

  /**
   * Does this WDS deployment require a $WORKSPACE_ID environment variable to be set? When this env
   * var is set, the WDS deployment is tied to a single workspace, and all collections within this
   * WDS will be associated with that workspace.
   *
   * @return Does this WDS deployment require a $WORKSPACE_ID environment variable to be set?
   */
  public boolean getRequireEnvWorkspace() {
    return requireEnvWorkspace;
  }

  void setRequireEnvWorkspace(boolean requireEnvWorkspace) {
    this.requireEnvWorkspace = requireEnvWorkspace;
  }
}
