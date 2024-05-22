package org.databiosphere.workspacedataservice.dataimport.rawlsjson;

import java.util.Map;
import org.databiosphere.workspacedataservice.dataimport.ImportOptions;

public record RawlsJsonImportOptions(boolean isUpsert) implements ImportOptions {
  public static final String OPTION_IS_UPSERT = "isUpsert";

  public static RawlsJsonImportOptions from(Map<String, Object> options) {
    boolean isUpsert =
        Boolean.parseBoolean(options.getOrDefault(OPTION_IS_UPSERT, "true").toString());
    return new RawlsJsonImportOptions(isUpsert);
  }
}
