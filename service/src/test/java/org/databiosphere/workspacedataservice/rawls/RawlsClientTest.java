package org.databiosphere.workspacedataservice.rawls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.workspace.model.CloningInstructionsEnum;
import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@DirtiesContext
@ActiveProfiles(value = "control-plane")
@SpringBootTest
@TestPropertySource(
    properties = {
      // URI parsing requires a valid hostname here, even if we don't contact this host
      "rawlsUrl=https://localhost/",
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false",
      "sentry.dsn=https://ingest.sentry.io"
    })
class RawlsClientTest {
  @MockBean RawlsApi mockRawlsApi;

  @Autowired RawlsClient rawlsClient;

  @BeforeEach
  void beforeEach() {
    reset(mockRawlsApi);
  }

  @Captor ArgumentCaptor<NamedDataRepoSnapshot> namedDataRepoSnapshotCaptor;

  @Test
  void linkSnapshot() {
    // ARRANGE
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    UUID snapshotId = UUID.randomUUID();

    when(mockRawlsApi.createDataRepoSnapshotByWorkspaceId(eq(workspaceId.id()), any()))
        .thenAnswer((Answer<?>) invocation -> null);

    // ACT
    rawlsClient.createSnapshotReference(workspaceId.id(), snapshotId);

    // ASSERT
    verify(mockRawlsApi)
        .createDataRepoSnapshotByWorkspaceId(
            eq(workspaceId.id()), namedDataRepoSnapshotCaptor.capture());
    NamedDataRepoSnapshot snapshotReference = namedDataRepoSnapshotCaptor.getValue();
    assertEquals(snapshotId, snapshotReference.snapshotId());
    assertEquals("%s-policy".formatted(snapshotId), snapshotReference.name());
    assertEquals(CloningInstructionsEnum.REFERENCE, snapshotReference.cloningInstructions());
    assertThat(snapshotReference.properties())
        .containsEntry(WorkspaceManagerDao.PROP_PURPOSE, WorkspaceManagerDao.PURPOSE_POLICY);
  }
}
