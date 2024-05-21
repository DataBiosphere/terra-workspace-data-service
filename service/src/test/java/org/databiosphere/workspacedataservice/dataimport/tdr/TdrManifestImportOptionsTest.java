package org.databiosphere.workspacedataservice.dataimport.tdr;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TdrManifestImportOptionsTest {
  @Test
  void syncPermissionsDefaultsToFalse() {
    var importOptions = TdrManifestImportOptions.from(emptyMap());
    assertThat(importOptions.syncPermissions()).isFalse();
  }
}
