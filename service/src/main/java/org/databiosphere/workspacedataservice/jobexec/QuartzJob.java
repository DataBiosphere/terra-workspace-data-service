package org.databiosphere.workspacedataservice.jobexec;

import static org.databiosphere.workspacedataservice.generated.GenericJobServerModel.StatusEnum;
import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.util.UUID;
import org.databiosphere.workspacedataservice.config.DataImportProperties;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.service.MDCServletRequestListener;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.slf4j.MDC;

/**
 * WDS's base class for asynchronous Quartz jobs. Contains convenience methods and an overridable
 * best-practice implementation of `execute()`. This implementation:
 *
 * <p>- retrieves the job id as a UUID (Quartz stores it as a String)
 *
 * <p>- sets the WDS job to RUNNING
 *
 * <p>- calls the implementing class's `executeInternal()` method
 *
 * <p>- sets the WDS job to SUCCEEDED once `executeInternal()` finishes
 *
 * <p>- sets the WDS job to FAILED on any Exception from `executeInternal()`
 *
 * <p>Note this implements Quartz's `Job` interface, not WDS's own `Job` model.
 */
// note this implements Quartz's `Job`, not WDS's own `Job`
public abstract class QuartzJob implements Job {

  private final JobDao jobDao;
  private final ObservationRegistry observationRegistry;
  private final DataImportProperties dataImportProperties;

  protected QuartzJob(
      JobDao jobDao,
      ObservationRegistry observationRegistry,
      DataImportProperties dataImportProperties) {
    this.jobDao = jobDao;
    this.observationRegistry = observationRegistry;
    this.dataImportProperties = dataImportProperties;
  }

  /**
   * implementing classes should override to annotate the observation with additional information
   */
  protected abstract void annotateObservation(Observation observation);

  @Override
  public void execute(JobExecutionContext context) throws org.quartz.JobExecutionException {
    // retrieve jobId
    JobKey jobKey = context.getJobDetail().getKey();
    UUID jobId = UUID.fromString(jobKey.getName());

    // (try to) set the MDC request id based on the originating thread
    JobDataMapReader jobData = JobDataMapReader.fromContext(context);
    propagateMdc(jobData);

    Observation observation =
        Observation.start("wds.job.execute", observationRegistry)
            .contextualName("job-execution")
            .highCardinalityKeyValue("jobId", jobId.toString());
    annotateObservation(observation);

    try {
      // mark this job as running
      jobDao.running(jobId);
      observation.event(Observation.Event.of("job.running"));
      // look for an auth token in the Quartz JobDataMap
      String authToken = jobData.getString(ARG_TOKEN);
      // and stash the auth token into job context
      JobContextHolder.init();
      JobContextHolder.setAttribute(ATTRIBUTE_NAME_TOKEN, authToken);

      // execute the specifics of this job
      executeInternal(jobId, context, observation);

      // if we reached here, and config says we should, mark this job as successful
      if (dataImportProperties.isSucceedOnCompletion()) {
        jobDao.succeeded(jobId);
        observation.lowCardinalityKeyValue("outcome", StatusEnum.SUCCEEDED.getValue());
      } else {
        // ensure we give the observation an outcome, even though we left the job running
        observation.lowCardinalityKeyValue("outcome", StatusEnum.RUNNING.getValue());
      }
    } catch (Exception e) {
      // on any otherwise-unhandled exception, mark the job as failed
      jobDao.fail(jobId, e);
      observation.error(e);
      observation.lowCardinalityKeyValue("outcome", StatusEnum.ERROR.getValue());
    } finally {
      JobContextHolder.destroy();
      observation.stop();
    }
  }

  protected abstract void executeInternal(
      UUID jobId, JobExecutionContext context, Observation observation);

  // try to retrieve MDC id from job context and add to this thread; don't fail if this errors out
  private void propagateMdc(JobDataMapReader reader) {
    try {
      String requestId = reader.getString(MDCServletRequestListener.MDC_KEY);
      MDC.put(MDCServletRequestListener.MDC_KEY, requestId);
    } catch (Exception e) {
      // noop
    }
  }
}
