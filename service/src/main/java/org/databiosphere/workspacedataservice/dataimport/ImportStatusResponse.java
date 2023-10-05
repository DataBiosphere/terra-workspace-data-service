package org.databiosphere.workspacedataservice.dataimport;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.databiosphere.workspacedataservice.generated.ImportJobStatusServerModel;

/**
 * Extends the generated model class ImportJobStatusServerModel. This extension allows us to specify
 * json serialization behavior. Currently, we exclude nulls from serialization, so if the response
 * has no errorMessage, we don't return the errorMessage key at all.
 *
 * <p>specify @JsonInclude(value=Include.NON_EMPTY, content=Include.NON_NULL) to omit empty objects
 * like `result: {}` from the response
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ImportStatusResponse extends ImportJobStatusServerModel {
  // TODO: add appropriate constructors so this isn't marked as deprecated
}
