package org.databiosphere.workspacedataservice.dao;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.databiosphere.workspacedataservice.TestUtils;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.databiosphere.workspacedataservice.workspace.WorkspaceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.transaction.annotation.Transactional;

@DirtiesContext
@SpringBootTest
class WorkspaceRepositoryTest extends ControlPlaneTestBase {
  @Autowired private WorkspaceRepository workspaceRepository;
  @Autowired private NamedParameterJdbcTemplate namedTemplate;

  @BeforeEach
  @AfterEach
  void clearAllWorkspaces() {
    // Delete all workspaces
    TestUtils.cleanAllWorkspaces(namedTemplate);
  }

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
        .isEqualTo(wdsDataTableType); // Should still be WDS since it saved only the first record
  }
}
