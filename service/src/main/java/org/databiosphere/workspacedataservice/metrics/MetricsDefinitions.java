package org.databiosphere.workspacedataservice.metrics;

public final class MetricsDefinitions {

  private MetricsDefinitions() {}

  // Record Metrics
  /** counter for column schema changes, i.e. "alter column" sql statements */
  public static final String COUNTER_COL_CHANGE = "column.change.datatype";

  /** tag for a {@link org.databiosphere.workspacedataservice.shared.model.RecordType} */
  public static final String TAG_RECORD_TYPE = "RecordType";

  /** tag for an instance id */
  public static final String TAG_INSTANCE = "Instance";

  /** tag for a record attribute name */
  public static final String TAG_ATTRIBUTE_NAME = "AttributeName";

  // Quartz Job Metrics
  /** observable name for running job */
  public static final String OBSERVE_JOB_EXECUTE = "wds.job.execute";

  /** event name for job succeeded */
  public static final String EVENT_JOB_SUCCEEDED = "job.succeeded";

  /** event name for job running */
  public static final String EVENT_JOB_RUNNING = "job.running";

  /** event name for job failed */
  public static final String EVENT_JOB_FAILED = "job.failed";

  /** name for job execution (used in span) */
  public static final String NAME_JOB_EXECUTION = "job-execution";

  /** tag for the type of job */
  public static final String TAG_JOB_TYPE = "jobType";

  /** tag for the type of job */
  public static final String TAG_JOB_ID = "jobId";
}
