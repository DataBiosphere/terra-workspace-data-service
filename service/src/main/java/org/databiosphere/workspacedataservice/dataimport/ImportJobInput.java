package org.databiosphere.workspacedataservice.dataimport;

import java.net.URI;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;

/** User-supplied input arguments for a data import job */
public class ImportJobInput implements JobInput {

  private final URI uri;
  private final ImportRequestServerModel.TypeEnum importType;

  public ImportJobInput(URI uri, ImportRequestServerModel.TypeEnum importType) {
    this.uri = uri;
    this.importType = importType;
  }

  public static ImportJobInput from(ImportRequestServerModel importRequest) {
    // if/when data imports need to include anything from importRequest.getOptions(), will need to
    // add them here, so they are persisted to the db
    return new ImportJobInput(importRequest.getUrl(), importRequest.getType());
  }

  public URI getUri() {
    return uri;
  }

  public ImportRequestServerModel.TypeEnum getImportType() {
    return importType;
  }
}
