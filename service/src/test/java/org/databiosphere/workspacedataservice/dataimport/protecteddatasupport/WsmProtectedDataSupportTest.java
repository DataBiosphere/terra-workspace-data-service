package org.databiosphere.workspacedataservice.dataimport.protecteddatasupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import bio.terra.workspace.model.WorkspaceDescription;
import bio.terra.workspace.model.WsmPolicyInput;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.common.DataPlaneTestBase;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerDao;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
class WsmProtectedDataSupportTest extends DataPlaneTestBase {
  @MockitoBean WorkspaceManagerDao wsmDao;

  @Autowired WsmProtectedDataSupport wsmProtectedDataSupport;

  @ParameterizedTest(name = "workspaceSupportsProtectedDataPolicy {1}")
  @MethodSource("workspaceSupportsProtectedDataPolicyTestCases")
  void workspaceSupportsProtectedDataPolicy(
      WorkspaceDescription workspaceDescription, boolean expectedToSupportProtectedDataPolicy) {
    // Arrange
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    when(wsmDao.getWorkspace(workspaceId)).thenReturn(workspaceDescription);

    // Act
    boolean supportsProtectedDataPolicy =
        wsmProtectedDataSupport.workspaceSupportsProtectedDataPolicy(workspaceId);

    // Assert
    assertThat(supportsProtectedDataPolicy).isEqualTo(expectedToSupportProtectedDataPolicy);
  }

  static Stream<Arguments> workspaceSupportsProtectedDataPolicyTestCases() {
    WorkspaceDescription workspaceWithProtectedDataPolicy = new WorkspaceDescription();
    WsmPolicyInput policy = new WsmPolicyInput();
    policy.setNamespace("terra");
    policy.setName("protected-data");
    workspaceWithProtectedDataPolicy.setPolicies(List.of(policy));

    WorkspaceDescription workspaceWithoutProtectedDataPolicy = new WorkspaceDescription();

    return Stream.of(
        Arguments.of(workspaceWithProtectedDataPolicy, true),
        Arguments.of(workspaceWithoutProtectedDataPolicy, false));
  }
}
