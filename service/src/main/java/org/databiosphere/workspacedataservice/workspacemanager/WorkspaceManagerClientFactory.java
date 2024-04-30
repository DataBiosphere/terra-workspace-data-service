package org.databiosphere.workspacedataservice.workspacemanager;

import bio.terra.workspace.api.ControlledAzureResourceApi;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import bio.terra.workspace.api.ResourceApi;
import bio.terra.workspace.api.WorkspaceApi;

public interface WorkspaceManagerClientFactory {
  ReferencedGcpResourceApi getReferencedGcpResourceApi(String authToken);

  ResourceApi getResourceApi(String authToken);

  ControlledAzureResourceApi getAzureResourceApi(String authToken);

  WorkspaceApi getWorkspaceApi(String authToken);
}
