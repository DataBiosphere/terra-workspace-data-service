package org.databiosphere.workspacedataservice.dataimport.rawlsjson;

import static org.junit.jupiter.api.Assertions.fail;

import org.databiosphere.workspacedataservice.common.TestBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("mock-sam")
class RawlsJsonQuartzJobTest extends TestBase {
  @Disabled("Not yet implemented")
  @Test
  void happyPath() {
    fail("Not yet implemented!");
  }
}
