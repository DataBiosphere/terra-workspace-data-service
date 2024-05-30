package org.databiosphere.workspacedataservice.service;

import static java.util.Objects.requireNonNullElse;

import com.google.common.collect.Sets;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;
import org.databiosphere.workspacedataservice.pubsub.JobStatusUpdate;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationMaskableException;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.service.model.exception.ValidationException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Service;

@Service
public class JobService {
  private static final Set<StatusEnum> TERMINAL_JOB_STATUSES =
      Set.of(StatusEnum.SUCCEEDED, StatusEnum.ERROR, StatusEnum.CANCELLED);

  public static final Set<StatusEnum> NONTERMINAL_JOB_STATUSES =
      Sets.difference(Set.of(StatusEnum.values()), TERMINAL_JOB_STATUSES);

  JobDao jobDao;
  CollectionService collectionService;

  public JobService(JobDao jobDao, CollectionService collectionService) {
    this.jobDao = jobDao;
    this.collectionService = collectionService;
  }

  public GenericJobServerModel getJob(UUID jobId) {
    try {
      GenericJobServerModel result = jobDao.getJob(jobId);
      if (!collectionService.canReadCollection(CollectionId.of(result.getInstanceId()))) {
        throw new AuthenticationMaskableException("Job");
      }
      return result;
    } catch (EmptyResultDataAccessException e) {
      throw new MissingObjectException("Job");
    }
  }

  public List<GenericJobServerModel> getJobsForCollection(
      CollectionId collectionId, Optional<List<String>> statuses) {
    // verify collection exists
    collectionService.validateCollection(collectionId);
    // check permissions
    if (!collectionService.canReadCollection(collectionId)) {
      throw new AuthenticationMaskableException("Collection");
    }
    return jobDao.getJobsForCollection(collectionId, statuses);
  }

  /**
   * Process a job status update from Rawls received via PubSub. This method is only used in the
   * control plane.
   */
  public void processStatusUpdate(JobStatusUpdate update) {
    UUID jobId = update.jobId();
    try {
      // JobService's getJob method checks permissions via Sam. Since this method is not run
      // as part of a user request, there is no token available to make requests to Sam with.
      // Thus, we must use jobDao's getJob here.
      GenericJobServerModel job = jobDao.getJob(jobId);
      StatusEnum currentStatus = job.getStatus();
      StatusEnum newStatus = update.newStatus();

      // Ignore messages that don't change the job's status.
      // Rawls and import service have more granular statuses than CWDS, so the initial update
      // will be from "ReadyToUpsert" to "Upserting", which both translate to "RUNNING" in CWDS.
      if (currentStatus.equals(newStatus)) {
        return;
      }

      // Once a job has reached a terminal state, do not update its status again.
      if (TERMINAL_JOB_STATUSES.contains(currentStatus)) {
        throw new ValidationException(
            "Unable to update terminal status for job %s".formatted(jobId));
      }

      switch (newStatus) {
        case SUCCEEDED -> jobDao.succeeded(jobId);
        case ERROR -> jobDao.fail(
            jobId, requireNonNullElse(update.errorMessage(), "Unknown error"));
        default -> throw new ValidationException(
            "Unexpected status from Rawls for job %s: %s".formatted(jobId, newStatus));
      }
    } catch (EmptyResultDataAccessException e) {
      throw new MissingObjectException("Job");
    }
  }
}
