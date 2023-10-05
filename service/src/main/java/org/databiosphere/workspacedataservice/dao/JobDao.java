package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.dataimport.ImportStatusResponse;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;

/** Interface for DAOs that read/write data import requests */
public interface ImportDao {

  void createImport(String jobId, ImportRequestServerModel importJob);

  void updateStatus(String jobId, JobStatus status);

  ImportStatusResponse getImport(String jobId);

  // TODO: save an errorMessage and stacktrace for an import job that hit a problem
}
