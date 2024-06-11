package org.databiosphere.workspacedataservice.dataimport.pfb;

import java.util.Map;
import org.databiosphere.workspacedataservice.dataimport.ImportOptions;

public record PfbImportOptions() implements ImportOptions {
  public static PfbImportOptions from(Map<String, Object> options) {
    return new PfbImportOptions();
  }
}
