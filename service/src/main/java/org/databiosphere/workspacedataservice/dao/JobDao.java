package org.databiosphere.workspacedataservice.dao;

import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;

public interface JobDao {

  GenericJobServerModel createJob(Job<JobInput, JobResult> job);

  GenericJobServerModel updateStatus(UUID jobId, GenericJobServerModel.StatusEnum status);

  GenericJobServerModel fail(UUID jobId, String errorMessage);

  GenericJobServerModel fail(UUID jobId, String errorMessage, StackTraceElement[] stackTrace);

  GenericJobServerModel getJob(UUID jobId);
}
