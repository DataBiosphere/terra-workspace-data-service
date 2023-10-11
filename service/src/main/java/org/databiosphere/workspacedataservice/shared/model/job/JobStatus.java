package org.databiosphere.workspacedataservice.shared.model.job;

import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;

/** The various states a Job can move through. */
public enum JobStatus {
  CREATED, // job has been created but not yet scheduled to run
  QUEUED, // job has been created and scheduled
  RUNNING, // job has started
  SUCCEEDED, // job completed as expected
  ERROR, // job failed
  CANCELLED, // job was interrupted or intentionally not started
  UNKNOWN;

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
}
