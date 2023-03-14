package org.databiosphere.workspacedataservice.service;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.service.model.RelationCollection;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.shared.model.*;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockMultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecordOrchestratorSamTest {

    @Autowired
    private RecordDao recordDao;
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
        if (!recordDao.instanceSchemaExists(INSTANCE)) {
            recordDao.createSchema(INSTANCE);
        }
        given(mockSamClientFactory.getResourcesApi())
                .willReturn(mockResourcesApi);

        // clear call history for the mock
        Mockito.clearInvocations(mockResourcesApi);
    }

    @AfterEach
    void cleanUp() {
        recordDao.dropSchema(INSTANCE);
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
}