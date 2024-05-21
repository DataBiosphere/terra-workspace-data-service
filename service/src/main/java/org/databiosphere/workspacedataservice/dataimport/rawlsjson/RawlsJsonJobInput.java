package org.databiosphere.workspacedataservice.dataimport.rawlsjson;

import java.io.Serial;
import java.net.URI;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;

public class RawlsJsonJobInput extends ImportJobInput {

  @Serial private static final long serialVersionUID = 0L;

  public RawlsJsonJobInput(
      URI uri, ImportRequestServerModel.TypeEnum importType, RawlsJsonImportOptions options) {
    super(uri, importType, options);
  }
}
