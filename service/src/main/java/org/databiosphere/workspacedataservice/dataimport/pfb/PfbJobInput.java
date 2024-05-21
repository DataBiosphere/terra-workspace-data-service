package org.databiosphere.workspacedataservice.dataimport.pfb;

import java.io.Serial;
import java.net.URI;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;

public class PfbJobInput extends ImportJobInput {

  @Serial private static final long serialVersionUID = 0L;

  public PfbJobInput(
      URI uri, ImportRequestServerModel.TypeEnum importType, PfbImportOptions options) {
    super(uri, importType, options);
  }
}
