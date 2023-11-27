package org.databiosphere.workspacedataservice.jobexec;

/** Exception thrown by asynchronous background jobs, i.e. Quartz jobs */
public class JobExecutionException extends RuntimeException {
  public JobExecutionException(String message) {
    super(message);
  }

  public JobExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
