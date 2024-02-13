package org.databiosphere.workspacedataservice.service;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.dao.CollectionDao;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.RestException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

/**
 * Tests for Sam exception handling inside CollectionService.createCollection and
 * CollectionService.deleteCollection.
 *
 * <p>Both createCollection and deleteCollection call Sam. If Sam returns an exception, we want
 * createCollection and deleteCollection to respond appropriately: - if Sam returns an ApiException
 * with status code 401, they should throw AuthenticationException. - if Sam returns an ApiException
 * with status code 403, they should throw AuthorizationException. - if Sam returns an ApiException
 * with a well-known status code like 404 or 503, they should throw a RestException with the same
 * status code. - if Sam returns an ApiException with a non-standard status code such as 0, which
 * happens in the case of a connection failure, they should throw a RestException with a 500 error
 * code. - if Sam returns some other exception such as NullPointerException, they should throw a
 * RestException with a 500 error code.
 */
@ActiveProfiles(profiles = "mock-collection-dao")
@DirtiesContext
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestPropertySource(
    properties = {
      "twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000"
    }) // example uuid from https://en.wikipedia.org/wiki/Universally_unique_identifier
class CollectionServiceSamExceptionTest {

  @Autowired private CollectionService collectionService;

  @Autowired private CollectionDao collectionDao;
  @Autowired private SamDao samDao;
  @Autowired private ActivityLogger activityLogger;

  // mock for the SamClientFactory; since this is a Spring bean we can use @MockBean
  @MockBean SamClientFactory mockSamClientFactory;

  // mock for the ResourcesApi class inside the Sam client; since this is not a Spring bean we have
  // to mock it manually
  final ResourcesApi mockResourcesApi = Mockito.mock(ResourcesApi.class);

  @Value("${twds.instance.workspace-id}")
  String containingWorkspaceId;

  @BeforeEach
  void setUp() {
    // return the mock ResourcesApi from the mock SamClientFactory
    given(mockSamClientFactory.getResourcesApi(null)).willReturn(mockResourcesApi);
  }

  @AfterEach
  void tearDown() {
    // clean up any collections left in the db
    List<UUID> allCollections = collectionDao.listCollectionSchemas();
    allCollections.forEach(collectionId -> collectionDao.dropSchema(collectionId));
  }

  @DisplayName(
      "if Sam throws ApiException(401) on resourcePermissionV2, createCollection and deleteCollection should throw AuthenticationException")
  @Test
  void testAuthenticationExceptionOnPermissionCheck() throws ApiException {
    int thrownStatusCode = 401;
    UUID collectionId = UUID.randomUUID();

    // Setup: the call to check permissions in Sam throws an ApiException
    given(
            mockResourcesApi.resourcePermissionV2(
                anyString(), eq(containingWorkspaceId), anyString()))
        .willThrow(
            new ApiException(
                thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode));

    doAuthnCreateAndDeleteTest(collectionId, AuthenticationException.class);
  }

  @DisplayName(
      "if Sam throws ApiException(403) on resourcePermissionV2, createCollection and deleteCollection should throw AuthorizationException")
  @Test
  void testAuthorizationExceptionOnPermissionCheck() throws ApiException {
    int thrownStatusCode = 403;
    UUID collectionId = UUID.randomUUID();

    // Setup: the call to check permissions in Sam throws an ApiException
    given(
            mockResourcesApi.resourcePermissionV2(
                anyString(), eq(containingWorkspaceId), anyString()))
        .willThrow(
            new ApiException(
                thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode));

    doAuthnCreateAndDeleteTest(collectionId, AuthorizationException.class);
  }

  @ParameterizedTest(
      name =
          "if Sam throws ApiException({0}) on resourcePermissionV2, createCollection and deleteCollection should throw RestException({0})")
  @ValueSource(ints = {400, 404, 409, 429, 500, 502, 503})
  void testStandardSamExceptionOnPermissionCheck(int thrownStatusCode) throws ApiException {
    UUID collectionId = UUID.randomUUID();

    // Setup: the call to check permissions in Sam throws an ApiException
    given(
            mockResourcesApi.resourcePermissionV2(
                anyString(), eq(containingWorkspaceId), anyString()))
        .willThrow(
            new ApiException(
                thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode));

    doSamCreateAndDeleteTest(collectionId, thrownStatusCode);
  }

  @ParameterizedTest(
      name =
          "if Sam throws ApiException({0}) on resourcePermissionV2, createCollection and deleteCollection should throw RestException(500)")
  @ValueSource(ints = {-1, 0, 8080})
  void testNonstandardSamExceptionOnPermissionCheck(int thrownStatusCode) throws ApiException {
    UUID collectionId = UUID.randomUUID();

    // Setup: the call to check permissions in Sam throws an ApiException
    given(
            mockResourcesApi.resourcePermissionV2(
                anyString(), eq(containingWorkspaceId), anyString()))
        .willThrow(
            new ApiException(
                thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode));

    doSamCreateAndDeleteTest(collectionId, 500);
  }

  @ParameterizedTest(
      name =
          "if Sam throws {0} on resourcePermissionV2, createCollection and deleteCollection should throw RestException(500)")
  @ValueSource(classes = {NullPointerException.class, RuntimeException.class})
  void testOtherExceptionOnPermissionCheck(Class<Throwable> clazz)
      throws ApiException,
          NoSuchMethodException,
          InvocationTargetException,
          InstantiationException,
          IllegalAccessException {
    UUID collectionId = UUID.randomUUID();

    Constructor<Throwable> ctor = clazz.getConstructor(String.class);
    Throwable toThrow = ctor.newInstance("intentional exception for unit test: " + clazz.getName());

    // Setup: the call to check permissions in Sam throws the specified Exception
    given(
            mockResourcesApi.resourcePermissionV2(
                anyString(), eq(containingWorkspaceId), anyString()))
        .willThrow(toThrow);

    doSamCreateAndDeleteTest(collectionId, 500);
  }

  // implementation of tests that expect AuthenticationException or AuthorizationException
  private void doAuthnCreateAndDeleteTest(
      UUID collectionId, Class<? extends Exception> expectedExceptionClass) {
    doAuthnCreateTest(collectionId, expectedExceptionClass);
    doAuthnDeleteTest(collectionId, expectedExceptionClass);
  }

  private void doAuthnCreateTest(
      UUID collectionId, Class<? extends Exception> expectedExceptionClass) {

    // attempt to create the collection, which should fail
    assertThrows(
        expectedExceptionClass,
        () -> collectionService.createCollection(collectionId, VERSION),
        "createCollection should throw if caller does not have write permission to the workspace resource in Sam");
    List<UUID> allCollections = collectionService.listCollections(VERSION);
    assertFalse(
        allCollections.contains(collectionId),
        "CollectionService.createCollection should not have created the collection.");
  }

  private void doAuthnDeleteTest(
      UUID collectionId, Class<? extends Exception> expectedExceptionClass) {
    // create the collection (directly in the db, bypassing Sam)
    collectionDao.createSchema(collectionId);
    List<UUID> allCollections = collectionService.listCollections(VERSION);
    assertTrue(
        allCollections.contains(collectionId), "unit test should have created the collections.");

    // attempt to delete the collection, which should fail
    assertThrows(
        expectedExceptionClass,
        () -> collectionService.deleteCollection(collectionId, VERSION),
        "deleteCollection should throw if caller does not have delete permission to the workspace resource in Sam");
    allCollections = collectionService.listCollections(VERSION);
    assertTrue(
        allCollections.contains(collectionId),
        "CollectionService.deleteCollection should not have deleted the collections.");
  }

  // implementation of tests that expect RestException
  private void doSamCreateAndDeleteTest(UUID collectionId, int expectedSamExceptionCode) {
    doSamCreateTest(collectionId, expectedSamExceptionCode);
    doSamDeleteTest(collectionId, expectedSamExceptionCode);
  }

  private void doSamCreateTest(UUID collectionId, int expectedSamExceptionCode) {
    // attempt to create the collection, which should fail
    RestException samException =
        assertThrows(
            RestException.class,
            () -> collectionService.createCollection(collectionId, VERSION),
            "createCollection should throw if caller does not have write permission to the workspace resource in Sam");
    assertEquals(
        expectedSamExceptionCode,
        samException.getStatusCode().value(),
        "RestException from createCollection should have same status code as the thrown ApiException");
    List<UUID> allCollections = collectionService.listCollections(VERSION);
    assertFalse(allCollections.contains(collectionId), "should not have created the collections.");
  }

  private void doSamDeleteTest(UUID collectionId, int expectedSamExceptionCode) {
    // bypass Sam and create the collection directly in the db
    collectionDao.createSchema(collectionId);
    List<UUID> allCollections = collectionService.listCollections(VERSION);
    assertTrue(
        allCollections.contains(collectionId), "unit test should have created the collections.");

    // now attempt to delete the collection, which should fail
    RestException samException =
        assertThrows(
            RestException.class,
            () -> collectionService.deleteCollection(collectionId, VERSION),
            "deleteCollection should throw if caller does not have delete permission to the workspace resource in Sam");
    assertEquals(
        expectedSamExceptionCode,
        samException.getStatusCode().value(),
        "RestException from deleteCollection should have same status code as the thrown ApiException");
    allCollections = collectionService.listCollections(VERSION);
    assertTrue(
        allCollections.contains(collectionId),
        "collectionService.deleteCollection should not have deleted the collections.");
  }
}
