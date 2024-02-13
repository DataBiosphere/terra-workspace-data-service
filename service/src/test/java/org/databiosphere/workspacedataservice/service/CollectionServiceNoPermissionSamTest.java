package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

import java.util.List;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles(profiles = "mock-instance-dao")
@DirtiesContext
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CollectionServiceNoPermissionSamTest {

  @Autowired private CollectionService collectionService;

  @Autowired private CollectionDao collectionDao;

  // mock for the SamClientFactory; since this is a Spring bean we can use @MockBean
  @MockBean SamClientFactory mockSamClientFactory;

  // mock for the ResourcesApi class inside the Sam client; since this is not a Spring bean we have
  // to mock it manually
  final ResourcesApi mockResourcesApi = Mockito.mock(ResourcesApi.class);

  @Test
  void testCreateInstanceNoPermission() throws ApiException {

    // return the mock ResourcesApi from the mock SamClientFactory
    given(mockSamClientFactory.getResourcesApi(null)).willReturn(mockResourcesApi);

    // Call to check permissions in Sam does not throw an exception, but returns false -
    // i.e. the current user does not have permission
    given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
        .willReturn(false);

    UUID instanceId = UUID.randomUUID();
    assertThrows(
        AuthorizationException.class,
        () -> collectionService.createCollection(instanceId, VERSION),
        "createInstance should throw if caller does not have permission to create wds-instance resource in Sam");
    List<UUID> allInstances = collectionService.listCollections(VERSION);
    assertFalse(allInstances.contains(instanceId), "should not have created the instance.");
  }

  @Test
  void testDeleteInstanceNoPermission() throws ApiException {

    // return the mock ResourcesApi from the mock SamClientFactory
    given(mockSamClientFactory.getResourcesApi(null)).willReturn(mockResourcesApi);

    // Call to check permissions in Sam does not throw an exception, but returns false -
    // i.e. the current user does not have permission
    given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
        .willReturn(false);

    UUID instanceId = UUID.randomUUID();
    // create the instance (directly in the db, bypassing Sam)
    collectionDao.createSchema(instanceId);

    assertThrows(
        AuthorizationException.class,
        () -> collectionService.deleteCollection(instanceId, VERSION),
        "deleteInstance should throw if caller does not have permission to delete wds-instance resource in Sam");
    List<UUID> allInstances = collectionService.listCollections(VERSION);
    assertTrue(allInstances.contains(instanceId), "should not have deleted the instance.");
  }
}
