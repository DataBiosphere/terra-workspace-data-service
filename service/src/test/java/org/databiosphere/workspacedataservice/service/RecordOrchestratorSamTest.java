package org.databiosphere.workspacedataservice.service;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.SamException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

@SpringBootTest
@ActiveProfiles(profiles = { "mock-sam", "mock-instance-dao" })
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RecordOrchestratorSamTest {

    @Autowired
    private InstanceDao instanceDao;
    @Autowired
    private RecordOrchestratorService recordOrchestratorService;
    // mock for the SamClientFactory; since this is a Spring bean we can use @MockBean
    @MockBean
    SamClientFactory mockSamClientFactory;

    // mock for the ResourcesApi class inside the Sam client; since this is not a Spring bean we have to mock it manually
    ResourcesApi mockResourcesApi = Mockito.mock(ResourcesApi.class);


    private static final UUID INSTANCE = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

    @BeforeEach
    void setUp() {
        if (!instanceDao.instanceSchemaExists(INSTANCE)) {
            instanceDao.createSchema(INSTANCE);
        }
        given(mockSamClientFactory.getResourcesApi(null))
                .willReturn(mockResourcesApi);

        // clear call history for the mock
        Mockito.reset(mockResourcesApi);
    }

    @AfterEach
    void cleanUp() {
        instanceDao.dropSchema(INSTANCE);
    }

    @Test
    void testValidateAndPermissionNoPermission() throws ApiException {

        // Call to check permissions in Sam does not throw an exception, but returns false -
        // i.e. the current user does not have permission
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(false);

        assertThrows(AuthorizationException.class,
                () -> recordOrchestratorService.validateAndPermissions(INSTANCE, VERSION),
                "validateAndPermissions should throw if caller does not have write permission in Sam"
        );
    }

    @Test
    void testValidateAndPermissionWithPermission() throws ApiException {

        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);

        assertDoesNotThrow(() -> recordOrchestratorService.validateAndPermissions(INSTANCE, VERSION),
                "validateAndPermissions should not throw if caller has write permission in Sam"
        );
    }

    @Test
    void testValidateAndPermissionWhenException() throws ApiException {
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willThrow(new ApiException(0, "intentional failure for unit test")); // 0 indicates a failed connection
        assertThrows(SamException.class,
                () -> recordOrchestratorService.validateAndPermissions(INSTANCE, VERSION),
                "validateAndPermissions should throw if caller does not have write permission in Sam");
    }
}