package org.databiosphere.workspacedataservice.service;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
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
        // TODO: this should return a 404 a la "job does not exist or you do not have permission to
        //     see it"
        throw new AuthorizationException("Caller does not have permission to view this job.");
      }
      return result;
    } catch (EmptyResultDataAccessException e) {
      throw new MissingObjectException("Job");
    }
  }

  public List<GenericJobServerModel> getJobsForCollection(
      CollectionId collectionId, List<String> statuses) {
    if (!collectionService.canReadCollection(collectionId)) {
      // TODO: this should return a 404 a la "job does not exist or you do not have permission to
      //     see it"
      // TODO: the error message here is incorrect w/r/t "this job"
      throw new AuthorizationException("Caller does not have permission to view this job.");
    }
    return jobDao.getJobsForCollection(collectionId, statuses);
  }
}
