package org.databiosphere.workspacedataservice.service;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.model.CreateResourceRequestV2;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.SamException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;

/**
 * Tests for Sam exception handling inside InstanceService.createInstance and InstanceService.deleteInstance.
 * <p>
 * Both createInstance and deleteInstance call Sam. If Sam returns an exception, we want createInstance and deleteInstance
 * to respond appropriately:
 *      - if Sam returns an ApiException with status code 401 or 403, they should throw AuthorizationException.
 *      - if Sam returns an ApiException with a well-known status code like 404 or 503, they should throw a SamException
 *          with the same status code.
 *      - if Sam returns an ApiException with a non-standard status code such as 0, which happens in the case of a
 *          connection failure, they should throw a SamException with a 500 error code.
 *      - if Sam returns some other exception such as NullPointerException, they should throw a SamException
 *          with a 500 error code.
 */
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InstanceServiceSamExceptionTest {

    @Autowired
    private InstanceService instanceService;

    @Autowired
    private RecordDao recordDao;

    // mock for the SamClientFactory; since this is a Spring bean we can use @MockBean
    @MockBean
    SamClientFactory mockSamClientFactory;

    // mock for the ResourcesApi class inside the Sam client; since this is not a Spring bean we have to mock it manually
    ResourcesApi mockResourcesApi = Mockito.mock(ResourcesApi.class);

    @BeforeEach
    void beforeEach() {
        // return the mock ResourcesApi from the mock SamClientFactory
        given(mockSamClientFactory.getResourcesApi())
                .willReturn(mockResourcesApi);
    }

    @AfterEach
    void afterEach() {
        // clean up any instances left in the db
        List<UUID> allInstances = recordDao.listInstanceSchemas();
        allInstances.forEach(instanceId ->
                recordDao.dropSchema(instanceId));
    }

    @ParameterizedTest(name = "if Sam throws ApiException({0}) on resourcePermissionV2, createInstance and deleteInstance should throw AuthorizationException")
    @ValueSource(ints = {401, 403})
    void testAuthorizationExceptionOnPermissionCheck(int thrownStatusCode) throws ApiException {
        UUID instanceId = UUID.randomUUID();

        // Setup: the call to check permissions in Sam throws an ApiException
        given(mockResourcesApi.resourcePermissionV2(anyString(), eq(instanceId.toString()), anyString()))
                .willThrow(new ApiException(thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode));

        doAuthorizationCreateAndDeleteTest(instanceId);
    }

    @ParameterizedTest(name = "if Sam throws ApiException({0}) on resourcePermissionV2, createInstance and deleteInstance should throw SamException({0})")
    @ValueSource(ints = {400, 404, 409, 429, 500, 502, 503})
    void testStandardSamExceptionOnPermissionCheck(int thrownStatusCode) throws ApiException {
        UUID instanceId = UUID.randomUUID();

        // Setup: the call to check permissions in Sam throws an ApiException
        given(mockResourcesApi.resourcePermissionV2(anyString(), eq(instanceId.toString()), anyString()))
                .willThrow(new ApiException(thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode));

        doSamCreateAndDeleteTest(instanceId, thrownStatusCode);
    }

    @ParameterizedTest(name = "if Sam throws ApiException({0}) on resourcePermissionV2, createInstance and deleteInstance should throw SamException(500)")
    @ValueSource(ints = {-1, 0, 8080})
    void testNonstandardSamExceptionOnPermissionCheck(int thrownStatusCode) throws ApiException {
        UUID instanceId = UUID.randomUUID();

        // Setup: the call to check permissions in Sam throws an ApiException
        given(mockResourcesApi.resourcePermissionV2(anyString(), eq(instanceId.toString()), anyString()))
                .willThrow(new ApiException(thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode));

        doSamCreateAndDeleteTest(instanceId, 500);
    }

    @ParameterizedTest(name = "if Sam throws {0} on resourcePermissionV2, createInstance and deleteInstance should throw SamException(500)")
    @ValueSource(classes = {NullPointerException.class, RuntimeException.class})
    void testOtherExceptionOnPermissionCheck(Class<Throwable> clazz) throws ApiException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        UUID instanceId = UUID.randomUUID();

        Constructor<Throwable> ctor = clazz.getConstructor(String.class);
        Throwable toThrow = ctor.newInstance("intentional exception for unit test: " + clazz.getName());

        // Setup: the call to check permissions in Sam throws the specified Exception
        given(mockResourcesApi.resourcePermissionV2(anyString(), eq(instanceId.toString()), anyString()))
                .willThrow(toThrow);

        doSamCreateAndDeleteTest(instanceId, 500);
    }

    @ParameterizedTest(name = "if Sam throws ApiException({0}) on createResourceV2, createInstance should throw AuthorizationException")
    @ValueSource(ints = {401, 403})
    void testAuthorizationExceptionOnCreateResource(int thrownStatusCode) throws ApiException {
        UUID instanceId = UUID.randomUUID();

        // Setup: the call to check permissions in Sam returns true,
        // but the call to create resource in Sam throws an ApiException
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);
        willThrow(new ApiException(thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode))
                .given(mockResourcesApi)
                .createResourceV2(eq(SamDao.RESOURCE_NAME_INSTANCE), any(CreateResourceRequestV2.class));

        doAuthorizationCreateTest(instanceId);
    }

    @ParameterizedTest(name = "if Sam throws ApiException({0}) on createResourceV2, createInstance should throw SamException({0})")
    @ValueSource(ints = {400, 404, 409, 429, 500, 502, 503})
    void testStandardSamExceptionOnCreateResource(int thrownStatusCode) throws ApiException {
        UUID instanceId = UUID.randomUUID();

        // Setup: the call to check permissions in Sam returns true,
        // but the call to create resource in Sam throws an ApiException
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);
        willThrow(new ApiException(thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode))
                .given(mockResourcesApi)
                .createResourceV2(eq(SamDao.RESOURCE_NAME_INSTANCE), any(CreateResourceRequestV2.class));

        doSamCreateTest(instanceId, thrownStatusCode);
    }

    @ParameterizedTest(name = "if Sam throws ApiException({0}) on createResourceV2, createInstance should throw SamException(500)")
    @ValueSource(ints = {-1, 0, 8080})
    void testNonstandardSamExceptionOnCreateResource(int thrownStatusCode) throws ApiException {
        UUID instanceId = UUID.randomUUID();

        // Setup: the call to check permissions in Sam returns true,
        // but the call to create resource in Sam throws an ApiException
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);
        willThrow(new ApiException(thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode))
                .given(mockResourcesApi)
                .createResourceV2(eq(SamDao.RESOURCE_NAME_INSTANCE), any(CreateResourceRequestV2.class));

        doSamCreateTest(instanceId, 500);

    }

    @ParameterizedTest(name = "if Sam throws {0} on createResourceV2, createInstance should throw SamException(500)")
    @ValueSource(classes = {NullPointerException.class, RuntimeException.class})
    void testOtherExceptionOnCreateResource(Class<Throwable> clazz) throws ApiException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        UUID instanceId = UUID.randomUUID();

        Constructor<Throwable> ctor = clazz.getConstructor(String.class);
        Throwable toThrow = ctor.newInstance("intentional exception for unit test: " + clazz.getName());

        // Setup: the call to check permissions in Sam returns true,
        // but the call to create resource in Sam throws the specified Exception
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);
        willThrow(toThrow)
                .given(mockResourcesApi)
                .createResourceV2(eq(SamDao.RESOURCE_NAME_INSTANCE), any(CreateResourceRequestV2.class));

        doSamCreateTest(instanceId, 500);
    }

    @ParameterizedTest(name = "if Sam throws ApiException({0}) on deleteResourceV2, deleteInstance should throw AuthorizationException")
    @ValueSource(ints = {401, 403})
    void testAuthorizationExceptionOnDeleteResource(int thrownStatusCode) throws ApiException {
        UUID instanceId = UUID.randomUUID();

        // Setup: the call to check permissions in Sam returns true,
        // but the call to delete resource in Sam throws an ApiException
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);
        willThrow(new ApiException(thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode))
                .given(mockResourcesApi)
                .deleteResourceV2(eq(SamDao.RESOURCE_NAME_INSTANCE), eq(instanceId.toString()));
        doAuthorizationDeleteTest(instanceId);
    }

    @ParameterizedTest(name = "if Sam throws ApiException({0}) on deleteResourceV2, deleteInstance should throw SamException({0})")
    @ValueSource(ints = {400, 404, 409, 429, 500, 502, 503})
    void testStandardSamExceptionOnDeleteResource(int thrownStatusCode) throws ApiException {
        UUID instanceId = UUID.randomUUID();

        // Setup: the call to check permissions in Sam returns true,
        // but the call to delete resource in Sam throws an ApiException
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);
        willThrow(new ApiException(thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode))
                .given(mockResourcesApi)
                .deleteResourceV2(eq(SamDao.RESOURCE_NAME_INSTANCE), eq(instanceId.toString()));

        doSamDeleteTest(instanceId, thrownStatusCode);
    }

    @ParameterizedTest(name = "if Sam throws ApiException({0}) on deleteResourceV2, deleteInstance should throw SamException(500)")
    @ValueSource(ints = {-1, 0, 8080})
    void testNonstandardSamExceptionOnDeleteResource(int thrownStatusCode) throws ApiException {
        UUID instanceId = UUID.randomUUID();

        // Setup: the call to check permissions in Sam returns true,
        // but the call to delete resource in Sam throws an ApiException
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);
        willThrow(new ApiException(thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode))
                .given(mockResourcesApi)
                .deleteResourceV2(eq(SamDao.RESOURCE_NAME_INSTANCE), eq(instanceId.toString()));

        doSamDeleteTest(instanceId, 500);

    }

    @ParameterizedTest(name = "if Sam throws {0} on deleteResourceV2, deleteInstance should throw SamException(500)")
    @ValueSource(classes = {NullPointerException.class, RuntimeException.class})
    void testOtherExceptionOnDeleteResource(Class<Throwable> clazz) throws ApiException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        UUID instanceId = UUID.randomUUID();

        Constructor<Throwable> ctor = clazz.getConstructor(String.class);
        Throwable toThrow = ctor.newInstance("intentional exception for unit test: " + clazz.getName());

        // Setup: the call to check permissions in Sam returns true,
        // but the call to create resource in Sam throws the specified Exception
        given(mockResourcesApi.resourcePermissionV2(anyString(), anyString(), anyString()))
                .willReturn(true);
        willThrow(toThrow)
                .given(mockResourcesApi)
                .deleteResourceV2(eq(SamDao.RESOURCE_NAME_INSTANCE), eq(instanceId.toString()));

        doSamDeleteTest(instanceId, 500);

    }


    // implementation of tests that expect AuthorizationException
    private void doAuthorizationCreateAndDeleteTest(UUID instanceId) {
        doAuthorizationCreateTest(instanceId);
        doAuthorizationDeleteTest(instanceId);
    }

    private void doAuthorizationCreateTest(UUID instanceId) {

        // attempt to create the instance, which should fail
        assertThrows(AuthorizationException.class,
                () -> instanceService.createInstance(instanceId, VERSION, Optional.empty()),
                "createInstance should throw if caller does not have permission to create wds-instance resource in Sam"
        );
        List<UUID> allInstances = instanceService.listInstances(VERSION);
        assertFalse(allInstances.contains(instanceId), "instanceService.createInstance should not have created the instances.");
    }

    private void doAuthorizationDeleteTest(UUID instanceId) {
        // create the instance (directly in the db, bypassing Sam)
        recordDao.createSchema(instanceId);
        List<UUID> allInstances = instanceService.listInstances(VERSION);
        assertTrue(allInstances.contains(instanceId), "unit test should have created the instances.");

        // attempt to delete the instance, which should fail
        assertThrows(AuthorizationException.class,
                () -> instanceService.deleteInstance(instanceId, VERSION),
                "deleteInstance should throw if caller does not have permission to create wds-instance resource in Sam"
        );
        allInstances = instanceService.listInstances(VERSION);
        assertTrue(allInstances.contains(instanceId), "instanceService.deleteInstance should not have deleted the instances.");
    }

    // implementation of tests that expect SamException
    private void doSamCreateAndDeleteTest(UUID instanceId, int expectedSamExceptionCode) {
        doSamCreateTest(instanceId, expectedSamExceptionCode);
        doSamDeleteTest(instanceId, expectedSamExceptionCode);
    }

    private void doSamCreateTest(UUID instanceId, int expectedSamExceptionCode) {
        // attempt to create the instance, which should fail
        SamException samException = assertThrows(SamException.class,
                () -> instanceService.createInstance(instanceId, VERSION, Optional.empty()),
                "createInstance should throw if caller does not have permission to create wds-instance resource in Sam"
        );
        assertEquals(expectedSamExceptionCode, samException.getRawStatusCode(),
                "SamException from createInstance should have same status code as the thrown ApiException");
        List<UUID> allInstances = instanceService.listInstances(VERSION);
        assertFalse(allInstances.contains(instanceId), "should not have created the instances.");
    }

    private void doSamDeleteTest(UUID instanceId, int expectedSamExceptionCode) {
        // bypass Sam and create the instance directly in the db
        recordDao.createSchema(instanceId);
        List<UUID> allInstances = instanceService.listInstances(VERSION);
        assertTrue(allInstances.contains(instanceId), "unit test should have created the instances.");

        // now attempt to delete the instance, which should fail
        SamException samException = assertThrows(SamException.class,
                () -> instanceService.deleteInstance(instanceId, VERSION),
                "deleteInstance should throw if caller does not have permission to create wds-instance resource in Sam"
        );
        assertEquals(expectedSamExceptionCode, samException.getRawStatusCode(),
                "SamException from deleteInstance should have same status code as the thrown ApiException");
        allInstances = instanceService.listInstances(VERSION);
        assertTrue(allInstances.contains(instanceId), "instanceService.deleteInstance should not have deleted the instances.");
    }

}
