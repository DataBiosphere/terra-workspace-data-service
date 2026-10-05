package org.databiosphere.workspacedataservice.dataimport;

import java.net.URI;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;

public interface ImportValidator {
  void validateUrl(URI url);

  void validateImport(ImportRequestServerModel importRequest, WorkspaceId destinationWorkspaceId);
}
