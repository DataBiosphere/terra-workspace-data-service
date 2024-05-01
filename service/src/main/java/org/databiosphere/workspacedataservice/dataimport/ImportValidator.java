package org.databiosphere.workspacedataservice.dataimport;

import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;

public interface ImportValidator {
  void validateImport(ImportRequestServerModel importRequest, UUID destinationWorkspaceId);
}
