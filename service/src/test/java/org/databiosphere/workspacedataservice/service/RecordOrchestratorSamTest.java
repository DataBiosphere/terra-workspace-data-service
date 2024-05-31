package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.databiosphere.workspacedataservice.shared.model.CollectionId;
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

@DirtiesContext
@SpringBootTest(
    properties = {
      "rest.retry.maxAttempts=2",
      "rest.retry.backoff.delay=10"
    }) // aggressive retry settings so unit test doesn't run too long)
@ActiveProfiles(profiles = {"mock-sam", "mock-collection-dao"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecordOrchestratorSamTest extends TestBase {

  @Autowired private CollectionDao collectionDao;
  @Autowired private RecordOrchestratorService recordOrchestratorService;
  // mock for the SamClientFactory; since this is a Spring bean we can use @MockBean
  @MockBean SamClientFactory mockSamClientFactory;

  // mock for the ResourcesApi class inside the Sam client; since this is not a Spring bean we have
  // to mock it manually
  final ResourcesApi mockResourcesApi = Mockito.mock(ResourcesApi.class);

  private static final UUID COLLECTION = UUID.fromString(HARDCODED_WORKSPACE_ID);

  @BeforeEach
  void setUp() {
    if (!collectionDao.collectionSchemaExists(CollectionId.of(COLLECTION))) {
      collectionDao.createSchema(COLLECTION);
    }
    given(mockSamClientFactory.getResourcesApi()).willReturn(mockResourcesApi);

    // clear call history for the mock
    Mockito.reset(mockResourcesApi);
  }

  @AfterEach
  void tearDown() {
    collectionDao.dropSchema(COLLECTION);
  }

  @Test
  void testValidateAndPermissionNoPermission() throws ApiException {

    // Call to check permissions in Sam does not throw an exception, but returns false -
    // i.e. the current user does not have permission
    given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
        .willReturn(false);

    assertThrows(
        AuthorizationException.class,
        () -> recordOrchestratorService.validateAndPermissions(COLLECTION, VERSION),
        "validateAndPermissions should throw if caller does not have write permission in Sam");
  }

  @Test
  void testValidateAndPermissionWithPermission() throws ApiException {

    given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
        .willReturn(true);

    assertDoesNotThrow(
        () -> recordOrchestratorService.validateAndPermissions(COLLECTION, VERSION),
        "validateAndPermissions should not throw if caller has write permission in Sam");
  }

  @Test
  void testValidateAndPermissionWhenException() throws ApiException {
    given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
        .willThrow(
            new ApiException(
                0, "intentional failure for unit test")); // 0 indicates a failed connection
    assertThrows(
        RestException.class,
        () -> recordOrchestratorService.validateAndPermissions(COLLECTION, VERSION),
        "validateAndPermissions should throw if caller does not have write permission in Sam");
  }
}
