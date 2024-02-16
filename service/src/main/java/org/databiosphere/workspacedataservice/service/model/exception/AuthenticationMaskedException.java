package org.databiosphere.workspacedataservice.service.model.exception;

import org.databiosphere.workspacedataservice.controller.GlobalExceptionHandler;

/**
 * An AuthenticationException that should be masked - typically as a 404 - in responses to end
 * users. Masking the underlying auth exception prevents end users from fishing for the
 * existence/non-existence of resources to which they don't have permission.
 *
 * <p>Masking happens for all Controllers in {@link GlobalExceptionHandler}
 *
 * @see GlobalExceptionHandler
 */
public class AuthenticationMaskedException extends AuthenticationException {

  private final String objectType;

  public AuthenticationMaskedException(String objectType) {
    super("Caller does not have permission to view this " + objectType);
    this.objectType = objectType;
  }

  public String getObjectType() {
    return objectType;
  }
}
