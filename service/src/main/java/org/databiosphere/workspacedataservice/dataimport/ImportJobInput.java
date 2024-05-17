package org.databiosphere.workspacedataservice.dataimport;

import static java.util.Collections.emptyMap;

import java.net.URI;
import java.util.Map;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.shared.model.job.JobInput;

/** User-supplied input arguments for a data import job */
public record ImportJobInput(
    URI uri, ImportRequestServerModel.TypeEnum importType, Map<String, Object> options)
    implements JobInput {

  public ImportJobInput(URI uri, ImportRequestServerModel.TypeEnum importType) {
    this(uri, importType, emptyMap());
  }

  public static ImportJobInput from(ImportRequestServerModel importRequest) {
    return new ImportJobInput(
        importRequest.getUrl(), importRequest.getType(), importRequest.getOptions());
  }
}
