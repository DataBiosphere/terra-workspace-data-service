package org.databiosphere.workspacedataservice.dataimport;

import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoopImportValidator implements ImportValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(NoopImportValidator.class);

  public void validateImport(ImportRequestServerModel importRequest) {
    LOGGER.warn("Skipping import validation.");
  }
}
