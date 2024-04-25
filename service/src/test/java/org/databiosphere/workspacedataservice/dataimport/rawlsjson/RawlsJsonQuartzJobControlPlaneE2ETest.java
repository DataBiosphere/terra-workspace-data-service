package org.databiosphere.workspacedataservice.dataimport.rawlsjson;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

/**
 * Tests for RAWLSJSON import that execute "end-to-end" - which for this format is relatively
 * simple, merely moving the import file from the given URI to an expected location without any
 * reprocessing and communicating that the new location to Rawls via pubsub.
 */
@ActiveProfiles(profiles = {"mock-sam", "noop-scheduler-dao", "control-plane"})
@DirtiesContext
@SpringBootTest
@TestPropertySource(
    properties = {
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false",
      // Allow file imports to test with files from resources.
      "twds.data-import.allowed-schemes=file",
      // Rawls url must be valid, else context initialization (Spring startup) will fail
      "rawlsUrl=https://localhost/",
    })
@AutoConfigureMockMvc
class RawlsJsonQuartzJobControlPlaneE2ETest {
  @Disabled("Not yet implemented")
  @Test
  void happyPath() {
    fail("Not yet implemented!");
  }
}
