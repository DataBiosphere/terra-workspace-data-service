package org.databiosphere.workspacedataservice.shared.model.job;

import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.springframework.http.HttpStatus;

/** The various states a Job can move through. */
public enum JobStatus {
  CREATED(HttpStatus.ACCEPTED, false, true), // job has been created but not yet scheduled to run
  QUEUED(HttpStatus.ACCEPTED, false, true), // job has been created and scheduled
  RUNNING(HttpStatus.ACCEPTED, false, true), // job has started
  SUCCEEDED(HttpStatus.OK, true, false), // job completed as expected
  ERROR(HttpStatus.OK, true, false), // job failed
  CANCELLED(HttpStatus.OK, true, false), // job was interrupted or intentionally not started
  UNKNOWN(HttpStatus.INTERNAL_SERVER_ERROR, false, false); // note neither running nor terminated

  private final HttpStatus code;
  private final boolean isTerminated;
  private final boolean isRunning;

  JobStatus(HttpStatus code, boolean isTerminated, boolean isRunning) {
    this.code = code;
    this.isTerminated = isTerminated;
    this.isRunning = isRunning;
  }

  // we have competing enums - this JobStatus class, which we have hand-coded and is in use
  // by backup, restore, and cloning legacy sync jobs; and the GenericJobServerModel.StatusEnum enum
  // which is auto-generated from our OpenAPI spec and is in use by the new "v1" async jobs.
  //
  // here, we provide translations between the two.
  public static JobStatus fromGeneratedModel(GenericJobServerModel.StatusEnum generatedStatus) {
    return JobStatus.valueOf(generatedStatus.name());
  }

  public GenericJobServerModel.StatusEnum toGeneratedModel() {
    return GenericJobServerModel.StatusEnum.valueOf(this.name());
  }

  public boolean isTerminated() {
    return this.isTerminated;
  }

  public boolean isRunning() {
    return this.isRunning;
  }

  public HttpStatus httpCode() {
    return this.code;
  }
}
