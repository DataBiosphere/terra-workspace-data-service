package org.databiosphere.workspacedataservice.dataimport.tdr;

import java.util.Map;
import org.databiosphere.workspacedataservice.dataimport.ImportOptions;

public record TdrManifestImportOptions(boolean syncPermissions) implements ImportOptions {
  public static final String OPTION_TDR_SYNC_PERMISSIONS = "tdrSyncPermissions";

  public static TdrManifestImportOptions from(Map<String, Object> options) {
    boolean syncPermissions =
        Boolean.parseBoolean(options.getOrDefault(OPTION_TDR_SYNC_PERMISSIONS, "false").toString());
    return new TdrManifestImportOptions(syncPermissions);
  }
}
