package org.databiosphere.workspacedataservice.dataimport;

import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoopImportValidator implements ImportValidator {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  public void validateImport(ImportRequestServerModel importRequest) {
    logger.warn("Skipping import validation.");
  }
}
