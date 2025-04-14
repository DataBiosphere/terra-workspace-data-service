package org.databiosphere.workspacedataservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.RawlsWorkspaceDetails;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@DirtiesContext
@SpringBootTest
class WorkspaceServiceControlPlaneTest extends ControlPlaneTestBase {

  @MockitoBean RawlsClient rawlsClient;
  @Autowired WorkspaceService workspaceService;

  static Stream<Arguments> workspaceTypeArguments() {
    return Stream.of(
        Arguments.of(
            RawlsWorkspaceDetails.RawlsWorkspace.WorkspaceType.MC, WorkspaceDataTableType.WDS),
        Arguments.of(
            RawlsWorkspaceDetails.RawlsWorkspace.WorkspaceType.RAWLS,
            WorkspaceDataTableType.RAWLS));
  }

  @ParameterizedTest(name = "workspace type `{0}` should use `{1}` data tables")
  @MethodSource("workspaceTypeArguments")
  void dataTableType(
      RawlsWorkspaceDetails.RawlsWorkspace.WorkspaceType workspaceType,
      WorkspaceDataTableType dataTableType) {

    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    RawlsWorkspaceDetails.RawlsWorkspace rawlsWorkspace =
        new RawlsWorkspaceDetails.RawlsWorkspace("bucketName", workspaceType, "namespace", "name");
    RawlsWorkspaceDetails rawlsWorkspaceDetails =
        new RawlsWorkspaceDetails(rawlsWorkspace, List.of());

    when(rawlsClient.getWorkspaceDetails(workspaceId.id())).thenReturn(rawlsWorkspaceDetails);

    WorkspaceDataTableType actual = workspaceService.getDataTableType(workspaceId);
    assertEquals(dataTableType, actual);
  }
}
