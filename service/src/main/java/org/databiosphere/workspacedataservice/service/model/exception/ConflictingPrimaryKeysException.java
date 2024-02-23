package org.databiosphere.workspacedataservice.service.model.exception;

public class ConflictingPrimaryKeysException extends ValidationException {
  public ConflictingPrimaryKeysException() {
    super("Primary key in payload does not match primary key in URL");
  }
}
