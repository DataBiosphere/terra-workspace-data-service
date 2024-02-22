package org.databiosphere.workspacedataservice.dao;

import org.databiosphere.workspacedataservice.shared.model.Schedulable;

/**
 * SchedulerDao implementation that does nothing, but throws no errors. Use this as a replacement
 * for QuartzSchedulerDao in unit tests where you need scheduling to succeed but don't want to
 * actually invoke Quartz.
 *
 * @see NoopSchedulerDaoConfig
 */
public class NoopSchedulerDao implements SchedulerDao {
  @Override
  public void schedule(Schedulable schedulable) {
    // do nothing.
  }
}
