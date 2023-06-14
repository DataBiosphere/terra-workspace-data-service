package org.databiosphere.workspacedataservice.workspacemanager;

import bio.terra.workspace.api.ControlledAzureResourceApi;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import bio.terra.workspace.api.ResourceApi;

public interface WorkspaceManagerClientFactory {
  ReferencedGcpResourceApi getReferencedGcpResourceApi();
  ResourceApi getResourceApi();
  ResourceApi getResourceApi(String token);
  ControlledAzureResourceApi getAzureResourceApi();
  ControlledAzureResourceApi getAzureResourceApi(String token);
}
