package org.databiosphere.workspacedataservice.dao;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.databiosphere.workspacedataservice.workspace.WorkspaceRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
class WorkspaceRepositoryTest {
  @Autowired private WorkspaceRepository workspaceRepository;

  @Test
  @Transactional
  void testSaveWorkspaceRecord() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    WorkspaceDataTableType wdsDataTableType = WorkspaceDataTableType.WDS;

    workspaceRepository.saveWorkspaceRecord(workspaceId, wdsDataTableType);

    WorkspaceRecord savedRecord = workspaceRepository.findById(workspaceId).orElse(null);
    assertThat(savedRecord).isNotNull();
    assertThat(savedRecord.getWorkspaceId()).isEqualTo(workspaceId);
    assertThat(savedRecord.getDataTableType()).isEqualTo(wdsDataTableType);
  }

  @Test
  @Transactional
  void testSaveWorkspaceRecordOnConflict() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    WorkspaceDataTableType wdsDataTableType = WorkspaceDataTableType.WDS;
    WorkspaceDataTableType rawlsDataTableType = WorkspaceDataTableType.RAWLS;

    workspaceRepository.saveWorkspaceRecord(workspaceId, wdsDataTableType);
    workspaceRepository.saveWorkspaceRecord(workspaceId, rawlsDataTableType);

    WorkspaceRecord savedRecord = workspaceRepository.findById(workspaceId).orElse(null);
    assertThat(savedRecord).isNotNull();
    assertThat(savedRecord.getWorkspaceId()).isEqualTo(workspaceId);
    assertThat(savedRecord.getDataTableType())
        .isEqualTo(wdsDataTableType); // Should still be WDS since it was the first record
  }
}
