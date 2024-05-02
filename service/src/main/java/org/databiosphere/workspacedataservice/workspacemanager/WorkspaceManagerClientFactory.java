package org.databiosphere.workspacedataservice.workspacemanager;

import bio.terra.workspace.api.ControlledAzureResourceApi;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import bio.terra.workspace.api.ResourceApi;
import bio.terra.workspace.api.WorkspaceApi;
import org.springframework.lang.Nullable;

public interface WorkspaceManagerClientFactory {
  ReferencedGcpResourceApi getReferencedGcpResourceApi(@Nullable String authToken);

  ResourceApi getResourceApi(@Nullable String authToken);

  ControlledAzureResourceApi getAzureResourceApi(@Nullable String authToken);

  WorkspaceApi getWorkspaceApi(@Nullable String authToken);
}
