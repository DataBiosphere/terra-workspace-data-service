package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.job.Job;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;
import org.databiosphere.workspacedataservice.shared.model.job.JobResult;

public interface SchedulerDao {

  void schedule(Job<JobInput, JobResult> job);
}
