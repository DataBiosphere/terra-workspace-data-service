package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.dataimport.ImportStatusResponse;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;

/** Interface for DAOs that read/write jobs */
public interface JobDao {

  void createJob(String jobId, ImportRequestServerModel importJob);

  void updateStatus(String jobId, JobStatus status);

  ImportStatusResponse getJob(String jobId);

  // TODO: save an errorMessage and stacktrace for an import job that hit a problem
}
