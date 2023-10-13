package org.databiosphere.workspacedataservice.shared.model;

import java.io.Serializable;
import java.util.Map;
import org.quartz.Job;
import org.quartz.JobDataMap;

public class Schedulable {

  // keys for job data arguments likely to be used by all/most Schedulables
  public static final String ARG_TOKEN = "authToken";
  public static final String ARG_INSTANCE = "instanceId";

  // classifier for types of schedulable jobs; feeds into Quartz's JobKey
  private final String group;
  // id for a single schedulable job; feeds into Quartz's JobKey
  private final String name;
  // for human-readable debugging
  private final String description;
  // Class to execute for this schedulable job
  private final Class<? extends Job> implementation;
  // inputs to the schedulable job
  private final Map<String, Serializable> arguments;

  public Schedulable(
      String group,
      String name,
      Class<? extends Job> implementation,
      String description,
      Map<String, Serializable> arguments) {
    this.group = group;
    this.name = name;
    this.implementation = implementation;
    this.description = description;
    this.arguments = arguments;
  }

  public String getGroup() {
    return group;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public Class<? extends Job> getImplementation() {
    return implementation;
  }

  public Map<String, Serializable> getArguments() {
    return arguments;
  }

  public JobDataMap getArgumentsAsJobDataMap() {
    JobDataMap jobDataMap = new JobDataMap();
    jobDataMap.putAll(arguments);
    return jobDataMap;
  }
}
