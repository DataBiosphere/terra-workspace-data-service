package org.databiosphere.workspacedataservice.workspace;

import static org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType.RAWLS;
import static org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType.WDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.dao.WorkspaceRepository;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.RawlsWorkspaceDetails;
import org.databiosphere.workspacedataservice.rawls.RawlsWorkspaceDetails.RawlsWorkspace;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

// note: we don't need @SpringBootTest, so we omit it to keep tests as light as possible
class RawlsDataTableTypeInspectorTest {

  static Stream<Arguments> workspaceTypeArguments() {
    return Stream.of(
        Arguments.of(RawlsWorkspace.WorkspaceType.MC, WDS),
        Arguments.of(RawlsWorkspace.WorkspaceType.RAWLS, RAWLS));
  }

  @ParameterizedTest(name = "workspace type `{0}` should use `{1}` data tables")
  @MethodSource("workspaceTypeArguments")
  void dataTableTypeRemote(
      RawlsWorkspace.WorkspaceType workspaceType, WorkspaceDataTableType dataTableType) {

    // mock Rawls client
    RawlsClient mockRawlsClient = Mockito.mock(RawlsClient.class);

    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    RawlsWorkspace rawlsWorkspace =
        new RawlsWorkspace("bucketName", workspaceType, "namespace", "name");
    RawlsWorkspaceDetails rawlsWorkspaceDetails =
        new RawlsWorkspaceDetails(rawlsWorkspace, List.of());

    when(mockRawlsClient.getWorkspaceDetails(workspaceId.id())).thenReturn(rawlsWorkspaceDetails);

    // mock WorkspaceRepository, which doesn't find a row
    WorkspaceRepository mockWorkspaceRepository = Mockito.mock(WorkspaceRepository.class);
    when(mockWorkspaceRepository.findById(workspaceId)).thenReturn(Optional.empty());

    RawlsDataTableTypeInspector inspector =
        new RawlsDataTableTypeInspector(mockRawlsClient, mockWorkspaceRepository);
    WorkspaceDataTableType actual = inspector.getWorkspaceDataTableType(workspaceId);
    assertEquals(dataTableType, actual);
  }

  @ParameterizedTest(name = "workspace type `{0}` should use `{1}` data tables")
  @MethodSource("workspaceTypeArguments")
  void dataTableTypeLocal(
      RawlsWorkspace.WorkspaceType ignoredWorkspaceType, WorkspaceDataTableType dataTableType) {

    // mock Rawls client (unimplemented, but needs to exist)
    RawlsClient mockRawlsClient = Mockito.mock(RawlsClient.class);

    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // mock WorkspaceRepository, which returns a value
    WorkspaceRepository mockWorkspaceRepository = Mockito.mock(WorkspaceRepository.class);
    when(mockWorkspaceRepository.findById(workspaceId))
        .thenReturn(Optional.of(new WorkspaceRecord(workspaceId, dataTableType)));

    RawlsDataTableTypeInspector inspector =
        new RawlsDataTableTypeInspector(mockRawlsClient, mockWorkspaceRepository);
    WorkspaceDataTableType actual = inspector.getWorkspaceDataTableType(workspaceId);
    assertEquals(dataTableType, actual);
  }

  @Test
  void useLocalResult() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // mock Rawls client
    RawlsClient mockRawlsClient = Mockito.mock(RawlsClient.class);
    // mock WorkspaceRepository
    WorkspaceRepository mockWorkspaceRepository = Mockito.mock(WorkspaceRepository.class);

    // WorkspaceRepository returns a value of RAWLS
    when(mockWorkspaceRepository.findById(workspaceId))
        .thenReturn(Optional.of(new WorkspaceRecord(workspaceId, RAWLS)));

    // Rawls client returns a value of MC
    RawlsWorkspace rawlsWorkspace =
        new RawlsWorkspace("bucketName", RawlsWorkspace.WorkspaceType.MC, "namespace", "name");
    RawlsWorkspaceDetails rawlsWorkspaceDetails =
        new RawlsWorkspaceDetails(rawlsWorkspace, List.of());

    when(mockRawlsClient.getWorkspaceDetails(workspaceId.id())).thenReturn(rawlsWorkspaceDetails);

    // invoke the inspector
    RawlsDataTableTypeInspector inspector =
        new RawlsDataTableTypeInspector(mockRawlsClient, mockWorkspaceRepository);
    WorkspaceDataTableType actual = inspector.getWorkspaceDataTableType(workspaceId);
    // result should be RAWLS because that's what the local db returns
    assertEquals(RAWLS, actual);

    // we should not even have made a REST request to Rawls
    verify(mockRawlsClient, never()).getWorkspaceDetails(any());

    // and we should not have tried to (re-)persist the data to the local db
    verify(mockWorkspaceRepository, never()).saveWorkspaceRecord(any(), any());
  }

  @Test
  void useAndPersistRemoteResult() {
    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());

    // mock Rawls client
    RawlsClient mockRawlsClient = Mockito.mock(RawlsClient.class);
    // mock WorkspaceRepository
    WorkspaceRepository mockWorkspaceRepository = Mockito.mock(WorkspaceRepository.class);

    // WorkspaceRepository does not contain this workspace
    when(mockWorkspaceRepository.findById(workspaceId)).thenReturn(Optional.empty());

    // Rawls client returns a value of MC
    RawlsWorkspace rawlsWorkspace =
        new RawlsWorkspace("bucketName", RawlsWorkspace.WorkspaceType.MC, "namespace", "name");
    RawlsWorkspaceDetails rawlsWorkspaceDetails =
        new RawlsWorkspaceDetails(rawlsWorkspace, List.of());

    when(mockRawlsClient.getWorkspaceDetails(workspaceId.id())).thenReturn(rawlsWorkspaceDetails);

    // invoke the inspector
    RawlsDataTableTypeInspector inspector =
        new RawlsDataTableTypeInspector(mockRawlsClient, mockWorkspaceRepository);
    WorkspaceDataTableType actual = inspector.getWorkspaceDataTableType(workspaceId);
    // result should be WDS because that's the result from the remote REST call
    assertEquals(WDS, actual);

    // we should have made the one remote request to Rawls
    verify(mockRawlsClient, times(1)).getWorkspaceDetails(workspaceId.id());

    // and we should have saved the result to the local repository
    verify(mockWorkspaceRepository, times(1)).saveWorkspaceRecord(workspaceId, WDS);
  }
}
