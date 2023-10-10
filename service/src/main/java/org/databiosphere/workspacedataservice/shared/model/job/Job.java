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

  private final UUID jobId;
  private final JobType jobType;
  private JobStatus status;
  private String errorMessage;
  private LocalDateTime created, updated;
  private final T input;
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
