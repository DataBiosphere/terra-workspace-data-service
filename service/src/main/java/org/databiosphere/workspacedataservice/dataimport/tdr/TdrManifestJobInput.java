package org.databiosphere.workspacedataservice.dataimport.tdr;

import java.io.Serial;
import java.net.URI;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;

public class TdrManifestJobInput extends ImportJobInput {

  @Serial private static final long serialVersionUID = 0L;

  public TdrManifestJobInput(
      URI uri, ImportRequestServerModel.TypeEnum importType, TdrManifestImportOptions options) {
    super(uri, importType, options);
  }
}
