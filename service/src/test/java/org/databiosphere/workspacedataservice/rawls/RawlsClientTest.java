package org.databiosphere.workspacedataservice.rawls;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@DirtiesContext
@SpringBootTest
class RawlsClientTest extends ControlPlaneTestBase {
  @MockitoBean RawlsApi mockRawlsApi;

  @Autowired RawlsClient rawlsClient;

  @BeforeEach
  void beforeEach() {
    reset(mockRawlsApi);
  }

  @Captor ArgumentCaptor<NamedDataRepoSnapshot> namedDataRepoSnapshotCaptor;
  @Captor ArgumentCaptor<List<UUID>> uuidListCaptor;

  @Test
  void linkSnapshot() {
    // ARRANGE
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    UUID snapshotId = UUID.randomUUID();

    when(mockRawlsApi.createSnapshotsByWorkspaceIdV3(eq(workspaceId.id()), any()))
        .thenAnswer((Answer<?>) invocation -> null);

    // ACT
    rawlsClient.createSnapshotReference(workspaceId.id(), snapshotId);

    // ASSERT
    verify(mockRawlsApi)
        .createSnapshotsByWorkspaceIdV3(eq(workspaceId.id()), uuidListCaptor.capture());
    List<UUID> capturedSnapshotId = uuidListCaptor.getValue();
    assertEquals(snapshotId, capturedSnapshotId.get(0));
  }
}
