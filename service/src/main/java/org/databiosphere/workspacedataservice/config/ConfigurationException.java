package org.databiosphere.workspacedataservice.config;

/** Thrown when there's a problem with the application configuration. */
public class ConfigurationException extends RuntimeException {
  public ConfigurationException(String message) {
    super(message);
  }

  public ConfigurationException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
