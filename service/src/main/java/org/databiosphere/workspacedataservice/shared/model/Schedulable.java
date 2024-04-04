package org.databiosphere.workspacedataservice.shared.model;

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.Map;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;

public class Schedulable {

  // keys for job data arguments likely to be used by all/most Schedulables
  public static final String ARG_TOKEN = "authToken";
  public static final String ARG_COLLECTION = "collectionId";
  public static final String ARG_URL = "url";

  // classifier for types of schedulable jobs; feeds into Quartz's JobKey
  private final String group;
  // id for a single schedulable job; feeds into Quartz's JobKey
  private final String id;
  // for human-readable debugging
  private final String description;
  // Class to execute for this schedulable job
  private final Class<? extends Job> implementation;
  // inputs to the schedulable job
  private final Map<String, Serializable> arguments;

  public Schedulable(
      String group,
      String id,
      Class<? extends Job> implementation,
      String description,
      Map<String, Serializable> arguments) {
    this.group = group;
    this.id = id;
    this.implementation = implementation;
    this.description = description;
    this.arguments = arguments;
  }

  @VisibleForTesting
  public String getGroup() {
    return group;
  }

  @VisibleForTesting
  public String getId() {
    return id;
  }

  @VisibleForTesting
  public String getDescription() {
    return description;
  }

  @VisibleForTesting
  public Class<? extends Job> getImplementation() {
    return implementation;
  }

  @VisibleForTesting
  public Map<String, Serializable> getArguments() {
    return arguments;
  }

  public JobDetail getJobDetail() {
    return JobBuilder.newJob()
        .ofType(getImplementation())
        .withIdentity(new JobKey(id, group))
        .setJobData(new JobDataMap(arguments))
        .storeDurably(false) // delete from the quartz table after the job finishes
        .withDescription(getDescription())
        .build();
  }
}
