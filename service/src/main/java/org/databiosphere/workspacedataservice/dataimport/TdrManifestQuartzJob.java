package org.databiosphere.workspacedataservice.dataimport;

import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_INSTANCE;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_URL;

import bio.terra.datarepo.model.SnapshotExportResponseModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URL;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.jobexec.JobExecutionException;
import org.databiosphere.workspacedataservice.jobexec.QuartzJob;
import org.databiosphere.workspacedataservice.service.DataRepoService;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class TdrManifestQuartzJob extends QuartzJob {

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
  protected JobDao getJobDao() {
    return this.jobDao;
  }

  @Override
  protected void executeInternal(UUID jobId, JobExecutionContext context) {
    // retrieve the Quartz JobDataMap, which contains arguments for this execution
    JobDataMap jobDataMap = context.getMergedJobDataMap();

    // get instanceid from job data
    UUID instanceId = getJobDataUUID(jobDataMap, ARG_INSTANCE);

    // get the TDR manifest url from job data
    URL url = getJobDataUrl(jobDataMap, ARG_URL);

    // read manifest
    SnapshotExportResponseModel snapshotExportResponseModel;
    try {
      snapshotExportResponseModel = mapper.readValue(url, SnapshotExportResponseModel.class);
    } catch (IOException e) {
      throw new JobExecutionException(
          "Error reading TDR snapshot manifest: %s".formatted(e.getMessage()), e);
    }

    // get the snapshot id from the manifest
    UUID snapshotId = snapshotExportResponseModel.getSnapshot().getId();
    logger.info("Starting import of snapshot {} to instance {}", snapshotId, instanceId);
    // perform the import via DataRepoService
    // TODO: pass token to dataRepoService, or possibly stash it on the thread
    dataRepoService.importSnapshot(instanceId, snapshotId);
  }
}
