package org.databiosphere.workspacedataservice.shared.model.job;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Represents a long-running, probably asynchronous, process which moves through multiple states
 * before completing. Different jobs can specify different JobResult implementations to satisfy
 * their own response payloads.
 *
 * @param <T> the input arguments for this job.
 * @param <U> the response payload for this job.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Job<T extends JobInput, U extends JobResult> {

  /** id for this job. */
  private final UUID jobId;

  /** type of this job, e.g. PFB import vs. database backup; {@link JobType} */
  private final JobType jobType;

  /** status of the job; {@link JobStatus} */
  private JobStatus status;

  /** Short error message, if this job hit an exception. Designed to be displayed to end users. */
  private String errorMessage;

  /**
   * Full stack trace, if this job hit an exception. Designed to be hidden from end users, but
   * collected and stored for developer debugging. Job executors are responsible for catching
   * exceptions and populating the stack trace into the job.
   */
  private StackTraceElement[] stackTrace;

  /** Creation time and last-updated time of this job. */
  private LocalDateTime created, updated;

  /**
   * Inputs for this job. Can use JobInput.empty() for no input arguments. Job writers can extend
   * JobInput to create their own type-safe definitions for the arguments required by their job.
   */
  private final T input;

  /**
   * Output of this job. Job writers can extend JobResult to create their own type-safe definitions
   * of the expected output of their job.
   */
  private U result;

  public Job(
      UUID jobId,
      JobType jobType,
      JobStatus status,
      String errorMessage,
      LocalDateTime created,
      LocalDateTime updated,
      T input,
      U result) {
    this.jobId = jobId;
    this.jobType = jobType;
    this.status = status;
    this.errorMessage = errorMessage;
    this.created = created;
    this.updated = updated;
    this.input = input;
    this.result = result;
  }

  // getters and setters

  public JobStatus getStatus() {
    return status;
  }

  public void setStatus(JobStatus status) {
    this.status = status;
  }

  public UUID getJobId() {
    return jobId;
  }

  public JobType getJobType() {
    return jobType;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public StackTraceElement[] getStackTrace() {
    return stackTrace;
  }

  public void setStackTrace(StackTraceElement[] stackTrace) {
    this.stackTrace = stackTrace;
  }

  public LocalDateTime getCreated() {
    return created;
  }

  public void setCreated(LocalDateTime created) {
    this.created = created;
  }

  public LocalDateTime getUpdated() {
    return updated;
  }

  public void setUpdated(LocalDateTime updated) {
    this.updated = updated;
  }

  public T getInput() {
    return input;
  }

  public U getResult() {
    return result;
  }

  public void setResult(U result) {
    this.result = result;
  }
}
