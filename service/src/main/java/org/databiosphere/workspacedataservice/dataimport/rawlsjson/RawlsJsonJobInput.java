package org.databiosphere.workspacedataservice.dataimport.rawlsjson;

import java.io.Serial;
import java.net.URI;
import org.databiosphere.workspacedataservice.dataimport.ImportJobInput;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel;
import org.databiosphere.workspacedataservice.generated.ImportRequestServerModel.TypeEnum;

public class RawlsJsonJobInput extends ImportJobInput {

  @Serial private static final long serialVersionUID = 0L;

  public RawlsJsonJobInput(URI uri, RawlsJsonImportOptions options) {
    super(uri, TypeEnum.RAWLSJSON, options);
  }

  public static RawlsJsonJobInput from(ImportRequestServerModel importRequest) {
    return new RawlsJsonJobInput(
        importRequest.getUrl(), RawlsJsonImportOptions.from(importRequest.getOptions()));
  }
}
