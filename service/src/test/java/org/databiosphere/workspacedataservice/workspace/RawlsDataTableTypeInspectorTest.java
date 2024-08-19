package org.databiosphere.workspacedataservice.workspace;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.RawlsWorkspaceDetails;
import org.databiosphere.workspacedataservice.rawls.RawlsWorkspaceDetails.RawlsWorkspace;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

// note: we don't need @SpringBootTest, so we omit it to keep tests as light as possible
class RawlsDataTableTypeInspectorTest {

  static Stream<Arguments> workspaceTypeArguments() {
    return Stream.of(
        Arguments.of(RawlsWorkspace.WorkspaceType.MC, WorkspaceDataTableType.WDS),
        Arguments.of(RawlsWorkspace.WorkspaceType.RAWLS, WorkspaceDataTableType.RAWLS));
  }

  @ParameterizedTest(name = "workspace type `{0}` should use `{1}` data tables")
  @MethodSource("workspaceTypeArguments")
  void dataTableType(
      RawlsWorkspace.WorkspaceType workspaceType, WorkspaceDataTableType dataTableType) {

    RawlsClient mockRawlsClient = Mockito.mock(RawlsClient.class);

    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    RawlsWorkspace rawlsWorkspace = new RawlsWorkspace("bucketName", workspaceType);
    RawlsWorkspaceDetails rawlsWorkspaceDetails =
        new RawlsWorkspaceDetails(rawlsWorkspace, List.of());

    when(mockRawlsClient.getWorkspaceDetails(workspaceId.id())).thenReturn(rawlsWorkspaceDetails);

    RawlsDataTableTypeInspector inspector = new RawlsDataTableTypeInspector(mockRawlsClient);
    WorkspaceDataTableType actual = inspector.getWorkspaceDataTableType(workspaceId);
    assertEquals(dataTableType, actual);
  }
}
