package org.databiosphere.workspacedataservice.dataimport.rawlsjson;

import static org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum.RAWLSJSON;
import static org.databiosphere.workspacedataservice.shared.model.job.JobType.DATA_IMPORT;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.util.UUID;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.jobexec.QuartzJob;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawlsJsonQuartzJob extends QuartzJob {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final JobDao jobDao;

  public RawlsJsonQuartzJob(
      DataImportProperties dataImportProperties,
      ObservationRegistry observationRegistry,
      JobDao jobDao) {
    super(observationRegistry, dataImportProperties);
    this.jobDao = jobDao;
  }

  @Override
  protected JobDao getJobDao() {
    return this.jobDao;
  }

  @Override
  protected void annotateObservation(Observation observation) {
    observation.lowCardinalityKeyValue("jobType", DATA_IMPORT.toString());
    observation.lowCardinalityKeyValue("importType", RAWLSJSON.toString());
  }

  @Override
  protected void executeInternal(UUID jobId, JobExecutionContext context) {
    logger.atInfo().log(
        "RawlsJsonQuartzJob.executeInternal: jobId={} - not yet implemented (no op)!", jobId);
  }
}
