package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.job.Job;

public interface SchedulerDao {

  void schedule(Job<?, ?> job);
}
