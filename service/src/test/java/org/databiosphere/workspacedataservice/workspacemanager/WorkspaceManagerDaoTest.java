package org.databiosphere.workspacedataservice.workspacemanager;

import bio.terra.datarepo.model.SnapshotModel;
import bio.terra.workspace.api.ReferencedGcpResourceApi;
import bio.terra.workspace.client.ApiException;
import bio.terra.workspace.model.CloningInstructionsEnum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;


@ActiveProfiles(profiles = "mock-sam")
@SpringBootTest(classes = {WorkspaceManagerConfig.class})
class WorkspaceManagerDaoTest {

    @Autowired
    WorkspaceManagerDao workspaceManagerDao;

    @MockBean
    WorkspaceManagerClientFactory mockWorkspaceManagerClientFactory;

    ReferencedGcpResourceApi mockReferencedGcpResourceApi = Mockito.mock(ReferencedGcpResourceApi.class);

    @BeforeEach
    void beforeEach() {
        given(mockWorkspaceManagerClientFactory.getReferencedGcpResourceApi()).willReturn(mockReferencedGcpResourceApi);
    }

    @Test
    void testSnapshotReturned() throws ApiException {
        final SnapshotModel testSnapshot = new SnapshotModel().name("test snapshot").id(UUID.randomUUID());
        workspaceManagerDao.createDataRepoSnapshotReference(testSnapshot);
        verify(mockReferencedGcpResourceApi).createDataRepoSnapshotReference(argThat(a ->
            a.getSnapshot().getSnapshot().equals(testSnapshot.getId().toString()) &&
                a.getMetadata().getCloningInstructions().equals(CloningInstructionsEnum.LINK_REFERENCE) &&
                a.getMetadata().getName().startsWith(testSnapshot.getName())), any());
    }

    @Test
    void testErrorThrown() throws ApiException {
        final int statusCode = HttpStatus.UNAUTHORIZED.value();
        final SnapshotModel testSnapshot = new SnapshotModel().name("test snapshot").id(UUID.randomUUID());
        given(mockReferencedGcpResourceApi.createDataRepoSnapshotReference(any(), any()))
                .willThrow(new ApiException(statusCode, "Intentional error thrown for unit test"));
        var exception = assertThrows(WorkspaceManagerException.class, () -> workspaceManagerDao.createDataRepoSnapshotReference(testSnapshot));
        assertEquals(statusCode, exception.getRawStatusCode());
    }
}
