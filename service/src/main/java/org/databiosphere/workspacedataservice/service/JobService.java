package org.databiosphere.workspacedataservice.service;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Service;

@Service
public class JobService {

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
        // if caller lacks permission, throw MissingObjectException, so we don't leak information
        // about this job's existence or non-existence
        throw new MissingObjectException("Job");
      }
      return result;
    } catch (EmptyResultDataAccessException e) {
      throw new MissingObjectException("Job");
    }
  }

  public List<GenericJobServerModel> getJobsForCollection(
      CollectionId collectionId, List<String> statuses) {
    // verify collection exists
    collectionService.validateCollection(collectionId.id());
    // check permissions
    if (!collectionService.canReadCollection(collectionId)) {
      // if caller lacks permission, throw MissingObjectException, so we don't leak information
      // about this collection's existence or non-existence
      throw new MissingObjectException("Collection");
    }
    return jobDao.getJobsForCollection(collectionId, statuses);
  }
}
