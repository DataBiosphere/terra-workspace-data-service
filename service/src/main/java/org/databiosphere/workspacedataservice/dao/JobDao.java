package org.databiosphere.workspacedataservice.dao;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;

public interface JobDao {

  // TODO: AJ-1011 awkward that this API accepts `Job` but returns `GenericJobServerModel`;
  // we should standardize on one model
  GenericJobServerModel createJob(Job<JobInput, JobResult> job);

  GenericJobServerModel updateStatus(UUID jobId, GenericJobServerModel.StatusEnum status);

  GenericJobServerModel queued(UUID jobId);

  GenericJobServerModel running(UUID jobId);

  GenericJobServerModel succeeded(UUID jobId);

  GenericJobServerModel fail(UUID jobId, String errorMessage);

  GenericJobServerModel fail(UUID jobId, String errorMessage, Exception e);

  GenericJobServerModel fail(UUID jobId, Exception e);

  GenericJobServerModel markError(UUID jobId, String errorMessage);

  GenericJobServerModel getJob(UUID jobId);

  List<GenericJobServerModel> getJobsForCollection(
      CollectionId collectionId, Optional<List<String>> statuses);

  List<GenericJobServerModel> getOldNonTerminalJobs();
}
