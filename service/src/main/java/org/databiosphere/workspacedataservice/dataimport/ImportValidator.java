package org.databiosphere.workspacedataservice.dataimport;

import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public interface ImportValidator {
  void validateImport(ImportRequestServerModel importRequest, WorkspaceId destinationWorkspaceId);
}
