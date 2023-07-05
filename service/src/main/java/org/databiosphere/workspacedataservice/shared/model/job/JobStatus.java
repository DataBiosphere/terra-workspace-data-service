package org.databiosphere.workspacedataservice.shared.model.job;

/**
 * The various states a Job can move through.
 */
public enum JobStatus {
    QUEUED,     // job has been created but not yet started
    RUNNING,    // job has started
    SUCCEEDED,  // job completed as expected
    ERROR,      // job failed
    CANCELLED,  // job was interrupted or intentionally not started
    UNKNOWN
}
