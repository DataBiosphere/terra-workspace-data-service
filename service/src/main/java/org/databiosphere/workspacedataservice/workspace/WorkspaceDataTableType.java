package org.databiosphere.workspacedataservice.workspace;

public enum WorkspaceDataTableType {
  // data tables powered by Rawls Entity Service; nothing persisted in WDS
  RAWLS,
  // data tables powered by WDS; persisted to WDS's Postgres
  WDS
}
