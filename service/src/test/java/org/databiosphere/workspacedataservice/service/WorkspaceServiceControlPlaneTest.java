package org.databiosphere.workspacedataservice.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.databiosphere.workspacedataservice.dao.JobDao;
import org.databiosphere.workspacedataservice.dao.WorkspaceRepository;
import org.databiosphere.workspacedataservice.rawls.RawlsClient;
import org.databiosphere.workspacedataservice.rawls.RawlsWorkspaceDetails;
import org.databiosphere.workspacedataservice.shared.model.WorkspaceId;
import org.databiosphere.workspacedataservice.workspace.DataTableTypeInspector;
import org.databiosphere.workspacedataservice.workspace.WorkspaceDataTableType;
import org.databiosphere.workspacedataservice.workspace.WorkspaceRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles(profiles = {"control-plane"})
@DirtiesContext
@SpringBootTest
@TestPropertySource(
    properties = {
      // turn off pubsub autoconfiguration for tests
      "spring.cloud.gcp.pubsub.enabled=false",
      // Rawls url must be valid, else context initialization (Spring startup) will fail
      "rawlsUrl=https://localhost/"
    })
class WorkspaceServiceControlPlaneTest {

  @MockBean RawlsClient rawlsClient;
  @Autowired WorkspaceService workspaceService;
  @Mock private WorkspaceRepository workspaceRepository;
  @Mock private DataTableTypeInspector dataTableTypeInspector;

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
        new RawlsWorkspaceDetails.RawlsWorkspace("bucketName", workspaceType);
    RawlsWorkspaceDetails rawlsWorkspaceDetails =
        new RawlsWorkspaceDetails(rawlsWorkspace, List.of());

    when(rawlsClient.getWorkspaceDetails(workspaceId.id())).thenReturn(rawlsWorkspaceDetails);

    WorkspaceDataTableType actual = workspaceService.getDataTableType(workspaceId);
    assertEquals(dataTableType, actual);
  }

  @ParameterizedTest(name = "Initialize `{0}` system workspace for `{1}-backed` data tables")
  @MethodSource("workspaceTypeArguments")
  void testInitSystemWorkspace_WhenWorkspaceNotExists(
      RawlsWorkspaceDetails.RawlsWorkspace.WorkspaceType workspaceType,
      WorkspaceDataTableType dataTableType) {

    JobDao mockJobDao = Mockito.mock(JobDao.class);
    CollectionService mockCollectionService = Mockito.mock(CollectionService.class);

    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    RawlsWorkspaceDetails.RawlsWorkspace rawlsWorkspace =
        new RawlsWorkspaceDetails.RawlsWorkspace("bucketName", workspaceType);
    RawlsWorkspaceDetails rawlsWorkspaceDetails =
        new RawlsWorkspaceDetails(rawlsWorkspace, List.of());
    WorkspaceRecord workspaceRecord = new WorkspaceRecord(workspaceId, dataTableType, true);

    when(rawlsClient.getWorkspaceDetails(workspaceId.id())).thenReturn(rawlsWorkspaceDetails);
    when(workspaceRepository.findById(workspaceId)).thenReturn(Optional.empty());
    when(dataTableTypeInspector.getWorkspaceDataTableType(workspaceId)).thenReturn(dataTableType);

    WorkspaceService mockWorkspaceService =
        new WorkspaceService(
            mockJobDao, mockCollectionService, dataTableTypeInspector, workspaceRepository);

    mockWorkspaceService.initSystemWorkspace(workspaceId);

    verify(workspaceRepository).save(workspaceRecord); // verify that workspaceRecord is saved
  }

  @ParameterizedTest(
      name = "Ignore Initializing `{0}` system workspace for existing `{1}-backed` data tables")
  @MethodSource("workspaceTypeArguments")
  void testInitSystemWorkspace_WhenWorkspaceExists(
      RawlsWorkspaceDetails.RawlsWorkspace.WorkspaceType workspaceType,
      WorkspaceDataTableType dataTableType) {

    JobDao mockJobDao = Mockito.mock(JobDao.class);
    CollectionService mockCollectionService = Mockito.mock(CollectionService.class);

    WorkspaceId workspaceId = WorkspaceId.of(UUID.randomUUID());
    RawlsWorkspaceDetails.RawlsWorkspace rawlsWorkspace =
        new RawlsWorkspaceDetails.RawlsWorkspace("bucketName", workspaceType);
    RawlsWorkspaceDetails rawlsWorkspaceDetails =
        new RawlsWorkspaceDetails(rawlsWorkspace, List.of());
    WorkspaceRecord workspaceRecord = new WorkspaceRecord(workspaceId, dataTableType, true);

    when(rawlsClient.getWorkspaceDetails(workspaceId.id())).thenReturn(rawlsWorkspaceDetails);
    when(workspaceRepository.findById(workspaceId)).thenReturn(Optional.of(workspaceRecord));
    when(dataTableTypeInspector.getWorkspaceDataTableType(workspaceId)).thenReturn(dataTableType);

    WorkspaceService mockWorkspaceService =
        new WorkspaceService(
            mockJobDao, mockCollectionService, dataTableTypeInspector, workspaceRepository);

    mockWorkspaceService.initSystemWorkspace(workspaceId);

    verify(workspaceRepository, never())
        .save(workspaceRecord); // verify that workspaceRecord is not saved
  }
}
