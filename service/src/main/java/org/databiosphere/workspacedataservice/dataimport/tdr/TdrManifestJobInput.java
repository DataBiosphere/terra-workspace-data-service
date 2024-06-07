package org.databiosphere.workspacedataservice.dataimport.tdr;

import java.io.Serial;
import java.net.URI;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;

public class TdrManifestJobInput extends ImportJobInput {

  @Serial private static final long serialVersionUID = 0L;

  public TdrManifestJobInput(URI uri, TdrManifestImportOptions options) {
    super(uri, TypeEnum.TDRMANIFEST, options);
  }

  public static TdrManifestJobInput from(ImportRequestServerModel importRequest) {
    return new TdrManifestJobInput(
        importRequest.getUrl(), TdrManifestImportOptions.from(importRequest.getOptions()));
  }
}
