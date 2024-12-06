package org.databiosphere.workspacedataservice.dataimport.protecteddatasupport;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import bio.terra.workspace.model.WsmPolicyInput;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.RawlsWorkspaceDetails;
import org.databiosphere.workspacedataservice.rawls.RawlsWorkspaceDetails.RawlsWorkspace.WorkspaceType;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
class RawlsProtectedDataSupportTest extends ControlPlaneTestBase {
  @MockitoBean RawlsClient rawlsClient;

  @Autowired RawlsProtectedDataSupport rawlsProtectedDataSupport;

  @ParameterizedTest(name = "{0} workspaceSupportsProtectedDataPolicy {3}")
  @MethodSource("workspaceSupportsProtectedDataPolicyTestCases")
  void workspaceSupportsProtectedDataPolicy(
      String testName,
      WorkspaceId workspaceId,
      RawlsWorkspaceDetails workspaceDetails,
      boolean expectedToSupportProtectedDataPolicy) {
    // Arrange
    when(rawlsClient.getWorkspaceDetails(workspaceId.id())).thenReturn(workspaceDetails);

    // Act
    boolean supportsProtectedDataPolicy =
        rawlsProtectedDataSupport.workspaceSupportsProtectedDataPolicy(workspaceId);

    // Assert
    assertThat(supportsProtectedDataPolicy).isEqualTo(expectedToSupportProtectedDataPolicy);
  }

  static Stream<Arguments> workspaceSupportsProtectedDataPolicyTestCases() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    WsmPolicyInput protectedDataPolicy = new WsmPolicyInput();
    protectedDataPolicy.setNamespace("terra");
    protectedDataPolicy.setName("protected-data");

    RawlsWorkspaceDetails azureProtectedWorkspace =
        new RawlsWorkspaceDetails(
            new RawlsWorkspaceDetails.RawlsWorkspace(null, WorkspaceType.MC),
            List.of(protectedDataPolicy));

    RawlsWorkspaceDetails azureUnprotectedWorkspace =
        new RawlsWorkspaceDetails(
            new RawlsWorkspaceDetails.RawlsWorkspace(null, WorkspaceType.MC), emptyList());

    RawlsWorkspaceDetails googleProtectedWorkspace =
        new RawlsWorkspaceDetails(
            new RawlsWorkspaceDetails.RawlsWorkspace(
                "fc-secure-%s".formatted(workspaceId.id()), WorkspaceType.RAWLS),
            emptyList());

    RawlsWorkspaceDetails googleUnprotectedWorkspace =
        new RawlsWorkspaceDetails(
            new RawlsWorkspaceDetails.RawlsWorkspace(
                "fc-%s".formatted(workspaceId.id()), WorkspaceType.RAWLS),
            emptyList());

    return Stream.of(
        Arguments.of("Azure", workspaceId, azureProtectedWorkspace, true),
        Arguments.of("Azure", workspaceId, azureUnprotectedWorkspace, false),
        Arguments.of("Google", workspaceId, googleProtectedWorkspace, true),
        Arguments.of("Google", workspaceId, googleUnprotectedWorkspace, false));
  }
}
