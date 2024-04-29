package org.databiosphere.workspacedataservice.dataimport;

import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;

public interface ImportValidator {
  void validateImport(ImportRequestServerModel importRequest);
}
