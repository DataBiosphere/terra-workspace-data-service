package org.databiosphere.workspacedataservice.shared.model.job;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/** Interface for the job-specific input arguments in Job. */
public interface JobInput {

  /**
   * an empty JobInput; that is, a JobInput that has no arguments. The explicit @JsonSerialize is
   * necessary here so Jackson knows to serialize this empty class. Jackson will write it as {}.
   * Alternately, we could set SerializationFeature.FAIL_ON_EMPTY_BEANS to false, but that's a
   * global change.
   */
  @JsonSerialize
  class Empty implements JobInput {}

  /** get an instance of JobInput.Empty */
  static Empty empty() {
    return new Empty();
  }
}
