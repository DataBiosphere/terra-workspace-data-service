package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.Schedulable;

public interface SchedulerDao {

  void schedule(Schedulable schedulable);
}
