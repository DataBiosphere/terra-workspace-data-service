package org.databiosphere.workspacedataservice.dataimport;

import org.databiosphere.workspacedataservice.jobexec.QuartzJob;

/** Base class for data-import Quartz jobs, containing constants and utilities for data imports. */
public abstract class ImportQuartzJob extends QuartzJob {
  public static final String ARG_URL = "url";
}
