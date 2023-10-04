package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.dataimport.ImportStatus;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;

/** Interface for DAOs that read/write data import requests */
public interface ImportDao {

  void createImport(String jobId, ImportRequestServerModel importJob);

  void updateStatus(String jobId, ImportStatus status);

  // TODO: get/describe an import job
  // TODO: save an errorMessage and stacktrace for an import job that hit a problem
}
