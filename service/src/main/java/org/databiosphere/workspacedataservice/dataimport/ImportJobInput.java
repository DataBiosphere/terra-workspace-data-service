package org.databiosphere.workspacedataservice.dataimport;

import java.net.URI;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;

/** User-supplied input arguments for a data import job */
public record ImportJobInput(URI uri, ImportRequestServerModel.TypeEnum importType)
    implements JobInput {

  public static ImportJobInput from(ImportRequestServerModel importRequest) {
    // if/when data imports need to include anything from importRequest.getOptions(), will need to
    // add them here, so they are persisted to the db
    return new ImportJobInput(importRequest.getUrl(), importRequest.getType());
  }
}
