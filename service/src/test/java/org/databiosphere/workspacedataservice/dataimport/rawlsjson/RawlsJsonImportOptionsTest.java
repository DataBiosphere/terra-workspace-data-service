package org.databiosphere.workspacedataservice.dataimport.rawlsjson;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RawlsJsonImportOptionsTest {
  @Test
  void isUpsertDefaultsToTrue() {
    var importOptions = RawlsJsonImportOptions.from(emptyMap());
    assertThat(importOptions.isUpsert()).isTrue();
  }
}
