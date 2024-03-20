package org.databiosphere.workspacedataservice.service;

import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

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
  private static final Set<StatusEnum> terminalJobStatuses =
      Set.of(StatusEnum.SUCCEEDED, StatusEnum.ERROR, StatusEnum.CANCELLED);

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
    collectionService.validateCollection(collectionId.id());
    // check permissions
    if (!collectionService.canReadCollection(collectionId)) {
      throw new AuthenticationMaskableException("Collection");
    }
    return jobDao.getJobsForCollection(collectionId, statuses);
  }

  /**
   * Process a job status update from Rawls received via PubSub. This method is only used in the
   * control plane (it's exposed through the control plane only PubSubController).
   */
  public void processStatusUpdate(JobStatusUpdate update) {
    try {
      UUID jobId = update.jobId();
      GenericJobServerModel job = getJob(jobId);
      StatusEnum currentStatus = job.getStatus();
      StatusEnum newStatus = update.newStatus();

      // Ignore messages that don't change the job's status.
      // Rawls and import service have more granular statuses than CWDS, so the initial update
      // will be from "ReadyToUpsert" to "Upserting", which both translate to "RUNNING" in CWDS.
      if (currentStatus.equals(newStatus)) {
        return;
      }

      // Once a job has reached a terminal state, do not update its status again.
      if (terminalJobStatuses.contains(currentStatus)) {
        throw new ValidationException(
            "Unable to update terminal status for job %s".formatted(jobId));
      }

      if (newStatus.equals(StatusEnum.ERROR)) {
        jobDao.fail(jobId, firstNonNull(update.errorMessage(), "Unknown error"));
      } else {
        jobDao.updateStatus(jobId, newStatus);
      }
    } catch (MissingObjectException e) {
      // Via PubSub, CWDS will receive status updates for both CWDS and import service jobs.
      // If the job is not found in the database (because it's an import service job), ignore it.
    }
  }
}
