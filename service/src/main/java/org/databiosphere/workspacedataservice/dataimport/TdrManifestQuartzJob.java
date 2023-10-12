package org.databiosphere.workspacedataservice.dataimport;

import bio.terra.datarepo.model.SnapshotExportResponseModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
import org.databiosphere.workspacedataservice.service.DataRepoService;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class TdrManifestQuartzJob implements Job {

  private final JobDao jobDao;
  private final ObjectMapper mapper;
  private final DataRepoService dataRepoService;

  private final Logger logger = LoggerFactory.getLogger(this.getClass());

  public TdrManifestQuartzJob(JobDao jobDao, ObjectMapper mapper, DataRepoService dataRepoService) {
    this.jobDao = jobDao;
    this.mapper = mapper;
    this.dataRepoService = dataRepoService;
  }

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    // retrieve jobId and mark this job as running
    JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
    UUID jobId = UUID.fromString(context.getJobDetail().getKey().getName());
    jobDao.updateStatus(jobId, GenericJobServerModel.StatusEnum.RUNNING);

    // get url to manifest from job data
    URL url;
    try {
      url = new URL(jobDataMap.getString("url"));
    } catch (MalformedURLException e) {
      jobDao.fail(jobId, e.getMessage(), e.getStackTrace());
      // TODO: dedicated exceptions for problems inside jobs
      throw new RuntimeException(e);
    }

    // get instanceid from job data
    UUID instanceId;
    try {
      instanceId = UUID.fromString(jobDataMap.getString("instanceId"));
    } catch (Exception e) {
      // TODO: make a fail() method that accepts an Exception
      // TODO: fail() methods should log
      jobDao.fail(jobId, e.getMessage(), e.getStackTrace());
      // TODO: dedicated exceptions for problems inside jobs
      throw new RuntimeException(e);
    }

    // read manifest
    SnapshotExportResponseModel snapshotExportResponseModel;
    try {
      snapshotExportResponseModel = mapper.readValue(url, SnapshotExportResponseModel.class);
    } catch (IOException e) {
      // TODO: dedicated exceptions for problems inside jobs
      jobDao.fail(jobId, e.getMessage(), e.getStackTrace());
      throw new RuntimeException(e);
    }

    UUID snapshotId = snapshotExportResponseModel.getSnapshot().getId();
    logger.info("Starting import of snapshot {} to instance {}", snapshotId, instanceId);
    // TODO: pass token to dataRepoService, or possibly stash it on the thread
    dataRepoService.importSnapshot(instanceId, snapshotId);
    jobDao.updateStatus(jobId, GenericJobServerModel.StatusEnum.SUCCEEDED);
  }
}
