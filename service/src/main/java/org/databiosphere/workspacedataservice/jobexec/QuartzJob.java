package org.databiosphere.workspacedataservice.jobexec;

import java.net.URL;
import java.util.UUID;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.generated.GenericJobServerModel;
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

  /** implementing classes are expected to be beans that inject a JobDao */
  JobDao jobDao;

  @Override
  public void execute(JobExecutionContext context) throws org.quartz.JobExecutionException {
    // retrieve jobId
    UUID jobId = UUID.fromString(context.getJobDetail().getKey().getName());
    try {
      // mark this job as running
      jobDao.updateStatus(jobId, GenericJobServerModel.StatusEnum.RUNNING);
      // execute the specifics of this job
      executeInternal(jobId, context);
      // if we reached here, mark this job as successful
      jobDao.updateStatus(jobId, GenericJobServerModel.StatusEnum.SUCCEEDED);
    } catch (Exception e) {
      // on any otherwise-unhandled exception, mark the job as failed
      jobDao.fail(jobId, e.getMessage(), e.getStackTrace());
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
