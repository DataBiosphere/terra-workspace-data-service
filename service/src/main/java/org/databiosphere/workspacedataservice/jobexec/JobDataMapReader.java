package org.databiosphere.workspacedataservice.jobexec;

import java.net.URL;
import java.util.UUID;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;

public class JobDataMapReader {

  private final JobDataMap jobDataMap;

  JobDataMapReader(JobDataMap jobDataMap) {
    this.jobDataMap = jobDataMap;
  }

  public static JobDataMapReader fromContext(JobExecutionContext context) {
    return new JobDataMapReader(context.getMergedJobDataMap());
  }

  /**
   * Retrieve a String value from a JobDataMap. Throws a JobExecutionException if the value is not
   * found/null or not a String.
   *
   * @param key where to find the String in the map
   * @return value from the JobDataMap
   */
  public String getString(String key) {
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
   * @param key where to find the UUID in the map
   * @return value from the JobDataMap
   */
  public UUID getUUID(String key) {
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
   * @param key where to find the URL in the map
   * @return value from the JobDataMap
   */
  public URL getURL(String key) {
    try {
      return new URL(jobDataMap.getString(key));
    } catch (Exception e) {
      throw new JobExecutionException(
          "Error retrieving key %s as URL from JobDataMap: %s".formatted(key, e.getMessage()), e);
    }
  }

  /**
   * Retrieve a Boolean from a JobDataMap. Throws a JobExecutionException if the value is not
   * found/null or cannot be parsed into a Boolean.
   */
  public Boolean getBoolean(String key) {
    try {
      return jobDataMap.getBooleanValue(key);
    } catch (Exception e) {
      throw new JobExecutionException(
          "Error retrieving key %s as Boolean from JobDataMap: %s".formatted(key, e.getMessage()),
          e);
    }
  }
}
