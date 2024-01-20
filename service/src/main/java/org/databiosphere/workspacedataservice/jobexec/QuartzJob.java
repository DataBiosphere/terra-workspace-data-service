package org.databiosphere.workspacedataservice.jobexec;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.databiosphere.workspacedataservice.shared.model.Schedulable.ARG_TOKEN;

import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.net.URL;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;

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

  private final ObservationRegistry observationRegistry;

  protected QuartzJob(ObservationRegistry observationRegistry) {
    this.observationRegistry = observationRegistry;
  }

  /** implementing classes are expected to be beans that inject a JobDao */
  protected abstract JobDao getJobDao();

  @Override
  public void execute(JobExecutionContext context) throws org.quartz.JobExecutionException {
    // retrieve jobId
    UUID jobId = UUID.fromString(context.getJobDetail().getKey().getName());
    // configure observation to collect counter, timer, and longtasktimer metrics
    observationRegistry
        .observationConfig()
        .observationHandler(new DefaultMeterObservationHandler(new SimpleMeterRegistry()));
    Observation observation =
        Observation.start("wds.job.execute", observationRegistry)
            .contextualName("job-execution")
            .lowCardinalityKeyValue("jobType", getClass().getSimpleName())
            .highCardinalityKeyValue("jobId", jobId.toString());
    try (Observation.Scope scope = observation.openScope()) {
      // mark this job as running
      getJobDao().running(jobId);
      observation.event(Observation.Event.of("job.running"));
      // look for an auth token in the Quartz JobDataMap
      String authToken = getJobDataString(context.getMergedJobDataMap(), ARG_TOKEN);
      // and stash the auth token into job context
      if (authToken != null) {
        JobContextHolder.init();
        JobContextHolder.setAttribute(ATTRIBUTE_NAME_TOKEN, authToken);
      }
      // execute the specifics of this job
      executeInternal(jobId, context);
      // if we reached here, mark this job as successful
      getJobDao().succeeded(jobId);
      observation.event(Observation.Event.of("job.succeeded"));
    } catch (Exception e) {
      // on any otherwise-unhandled exception, mark the job as failed
      getJobDao().fail(jobId, e);
      observation.error(e);
      observation.event(Observation.Event.of("job.failed"));
    } finally {
      JobContextHolder.destroy();
      observation.stop();
    }
  }

  protected abstract void executeInternal(UUID jobId, JobExecutionContext context);

  /**
   * Retrieve a String value from a JobDataMap. Throws a JobExecutionException if the value is not
   * found/null or not a String.
   *
   * @param jobDataMap the map from which to retrieve the String
   * @param key where to find the String in the map
   * @return value from the JobDataMap
   */
  protected String getJobDataString(JobDataMap jobDataMap, String key) {
    String returnValue;
    try {
      returnValue = jobDataMap.getString(key);
      if (returnValue == null) {
        throw new JobExecutionException("Key '%s' was null in JobDataMap".formatted(key));
      }
      return returnValue;
    } catch (Exception e) {
      throw new JobExecutionException(
          "Error retrieving key %s from JobDataMap: %s".formatted(key, e.getMessage()), e);
    }
  }

  /**
   * Retrieve a UUID value from a JobDataMap. Throws a JobExecutionException if the value is not
   * found/null or not a UUID.
   *
   * @param jobDataMap the map from which to retrieve the UUID
   * @param key where to find the UUID in the map
   * @return value from the JobDataMap
   */
  protected UUID getJobDataUUID(JobDataMap jobDataMap, String key) {
    try {
      return UUID.fromString(jobDataMap.getString(key));
    } catch (Exception e) {
      throw new JobExecutionException(
          "Error retrieving key %s as UUID from JobDataMap: %s".formatted(key, e.getMessage()), e);
    }
  }

  /**
   * Retrieve a URL value from a JobDataMap. Throws a JobExecutionException if the value is not
   * found/null or cannot be parsed into a URL.
   *
   * @param jobDataMap the map from which to retrieve the URL
   * @param key where to find the URL in the map
   * @return value from the JobDataMap
   */
  protected URL getJobDataUrl(JobDataMap jobDataMap, String key) {
    try {
      return new URL(jobDataMap.getString(key));
    } catch (Exception e) {
      throw new JobExecutionException(
          "Error retrieving key %s as URL from JobDataMap: %s".formatted(key, e.getMessage()), e);
    }
  }
}
