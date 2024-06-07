package org.databiosphere.workspacedataservice.dataimport.pfb;

import java.io.Serial;
import java.net.URI;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;

public class PfbJobInput extends ImportJobInput {

  @Serial private static final long serialVersionUID = 0L;

  public PfbJobInput(URI uri, PfbImportOptions options) {
    super(uri, TypeEnum.PFB, options);
  }

  public static PfbJobInput from(ImportRequestServerModel importRequest) {
    return new PfbJobInput(
        importRequest.getUrl(), PfbImportOptions.from(importRequest.getOptions()));
  }
}
