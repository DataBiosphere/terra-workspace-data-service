package org.databiosphere.workspacedataservice.sourcewds;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedata.model.BackupJob;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class CloneOlderWdsTest extends DataPlaneTestBase {

  @Autowired ObjectMapper objectMapper;

  @Test
  void deserializeBackupResponse() {
    // this is the response from a WDS v0.2.100 or earlier, prior to
    // the `jobType` field being required
    String backupResponse =
        """
          {
            "jobId": "e0e9341a-7e5d-4e1d-8410-ea242e91399f",
            "status": "SUCCEEDED",
            "created": "2023-10-30T19:33:36.22421",
            "updated": "2023-10-30T19:33:42.819065",
            "result": {
              "filename": "wdsservice/cloning/backup/ed0b4126-e607-4b03-8a01-c65334e98a4c-2023-10-30_19-33-36.sql",
              "requester": "f2fcc10d-fee3-409a-bdba-6a92bf201f40"
            }
          }""";

    // can we parse such a response into the generated BackupJob class?
    assertDoesNotThrow(() -> BackupJob.fromJson(backupResponse));
  }
}
