package org.databiosphere.workspacedataservice.dao;

import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;

public interface JobDao {

  GenericJobServerModel createJob(Job<JobInput, JobResult> job);

  GenericJobServerModel updateStatus(UUID jobId, GenericJobServerModel.StatusEnum status);

  GenericJobServerModel updateStatus(
      UUID jobId, GenericJobServerModel.StatusEnum status, String errorMessage);

  GenericJobServerModel updateStatus(
      UUID jobId,
      GenericJobServerModel.StatusEnum status,
      String errorMessage,
      StackTraceElement[] stackTrace);

  GenericJobServerModel getJob(UUID jobId);
}
