package org.databiosphere.workspacedataservice.workspacemanager;

import bio.terra.workspace.api.ReferencedGcpResourceApi;

public interface WorkspaceManagerClientFactory {
  ReferencedGcpResourceApi getReferencedGcpResourceApi();
}
