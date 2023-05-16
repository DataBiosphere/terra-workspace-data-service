package org.databiosphere.workspacedataservice.service;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.databiosphere.workspacedataservice.activitylog.ActivityLogger;
import org.databiosphere.workspacedataservice.activitylog.ActivityLoggerConfig;
import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.MockInstanceDaoConfig;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.sam.SamConfig;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthenticationException;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.SamException;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;

/**
 * Tests for Sam exception handling inside InstanceService.createInstance and InstanceService.deleteInstance.
 * <p>
 * Both createInstance and deleteInstance call Sam. If Sam returns an exception, we want createInstance and deleteInstance
 * to respond appropriately:
 *      - if Sam returns an ApiException with status code 401, they should throw AuthenticationException.
 *      - if Sam returns an ApiException with status code 403, they should throw AuthorizationException.
 *      - if Sam returns an ApiException with a well-known status code like 404 or 503, they should throw a SamException
 *          with the same status code.
 *      - if Sam returns an ApiException with a non-standard status code such as 0, which happens in the case of a
 *          connection failure, they should throw a SamException with a 500 error code.
 *      - if Sam returns some other exception such as NullPointerException, they should throw a SamException
 *          with a 500 error code.
 */
@ActiveProfiles(profiles = "mock-instance-dao")
@SpringBootTest(classes = { MockInstanceDaoConfig.class, SamConfig.class, ActivityLoggerConfig.class })
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestPropertySource(properties = {"twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000"}) // example uuid from https://en.wikipedia.org/wiki/Universally_unique_identifier
class InstanceServiceSamExceptionTest {

    private InstanceService instanceService;

    @Autowired private InstanceDao instanceDao;
    @Autowired private SamDao samDao;
    @Autowired private ActivityLogger activityLogger;

    // mock for the SamClientFactory; since this is a Spring bean we can use @MockBean
    @MockBean
    SamClientFactory mockSamClientFactory;

    // mock for the ResourcesApi class inside the Sam client; since this is not a Spring bean we have to mock it manually
    ResourcesApi mockResourcesApi = Mockito.mock(ResourcesApi.class);

    @Value("${twds.instance.workspace-id}")
    String containingWorkspaceId;

    @BeforeEach
    void beforeEach() {
        instanceService = new InstanceService(instanceDao, samDao, activityLogger);

        // return the mock ResourcesApi from the mock SamClientFactory
        given(mockSamClientFactory.getResourcesApi(null))
                .willReturn(mockResourcesApi);
    }

    @AfterEach
    void afterEach() {
        // clean up any instances left in the db
        List<UUID> allInstances = instanceDao.listInstanceSchemas();
        allInstances.forEach(instanceId ->
                instanceDao.dropSchema(instanceId));
    }

    @DisplayName("if Sam throws ApiException(401) on resourcePermissionV2, createInstance and deleteInstance should throw AuthenticationException")
    @Test
    void testAuthenticationExceptionOnPermissionCheck() throws ApiException {
        int thrownStatusCode = 401;
        UUID instanceId = UUID.randomUUID();

        // Setup: the call to check permissions in Sam throws an ApiException
        given(mockResourcesApi.resourcePermissionV2(anyString(), eq(containingWorkspaceId), anyString()))
                .willThrow(new ApiException(thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode));

        doAuthnCreateAndDeleteTest(instanceId, AuthenticationException.class);
    }

    @DisplayName("if Sam throws ApiException(403) on resourcePermissionV2, createInstance and deleteInstance should throw AuthorizationException")
    @Test
    void testAuthorizationExceptionOnPermissionCheck() throws ApiException {
        int thrownStatusCode = 403;
        UUID instanceId = UUID.randomUUID();

        // Setup: the call to check permissions in Sam throws an ApiException
        given(mockResourcesApi.resourcePermissionV2(anyString(), eq(containingWorkspaceId), anyString()))
                .willThrow(new ApiException(thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode));

        doAuthnCreateAndDeleteTest(instanceId, AuthorizationException.class);
    }

    @ParameterizedTest(name = "if Sam throws ApiException({0}) on resourcePermissionV2, createInstance and deleteInstance should throw SamException({0})")
    @ValueSource(ints = {400, 404, 409, 429, 500, 502, 503})
    void testStandardSamExceptionOnPermissionCheck(int thrownStatusCode) throws ApiException {
        UUID instanceId = UUID.randomUUID();

        // Setup: the call to check permissions in Sam throws an ApiException
        given(mockResourcesApi.resourcePermissionV2(anyString(), eq(containingWorkspaceId), anyString()))
                .willThrow(new ApiException(thrownStatusCode, "intentional exception for unit test: " + thrownStatusCode));

        doSamCreateAndDeleteTest(instanceId, thrownStatusCode);
    }

    @ParameterizedTest(name = "if Sam throws ApiException({0}) on resourcePermissionV2, createInstance and deleteInstance should throw SamException(500)")
    @ValueSource(ints = {-1, 0, 8080})
    void testNonstandardSamExceptionOnPermissionCheck(int thrownStatusCode) throws ApiException {
        UUID instanceId = UUID.randomUUID();

        // Setup: the call to check permissions in Sam throws an ApiException
        given(mockResourcesApi.resourcePermissionV2(anyString(), eq(containingWorkspaceId), anyString()))
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
        given(mockResourcesApi.resourcePermissionV2(anyString(), eq(containingWorkspaceId), anyString()))
                .willThrow(toThrow);

        doSamCreateAndDeleteTest(instanceId, 500);
    }

    // implementation of tests that expect AuthenticationException or AuthorizationException
    private void doAuthnCreateAndDeleteTest(UUID instanceId, Class<? extends Exception> expectedExceptionClass) {
        doAuthnCreateTest(instanceId, expectedExceptionClass);
        doAuthnDeleteTest(instanceId, expectedExceptionClass);
    }

    private void doAuthnCreateTest(UUID instanceId, Class<? extends Exception> expectedExceptionClass) {

        // attempt to create the instance, which should fail
        assertThrows(expectedExceptionClass,
                () -> instanceService.createInstance(instanceId, VERSION),
                "createInstance should throw if caller does not have permission to create wds-instance resource in Sam"
        );
        List<UUID> allInstances = instanceService.listInstances(VERSION);
        assertFalse(allInstances.contains(instanceId), "instanceService.createInstance should not have created the instances.");
    }

    private void doAuthnDeleteTest(UUID instanceId, Class<? extends Exception> expectedExceptionClass) {
        // create the instance (directly in the db, bypassing Sam)
        instanceDao.createSchema(instanceId);
        List<UUID> allInstances = instanceService.listInstances(VERSION);
        assertTrue(allInstances.contains(instanceId), "unit test should have created the instances.");

        // attempt to delete the instance, which should fail
        assertThrows(expectedExceptionClass,
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
                () -> instanceService.createInstance(instanceId, VERSION),
                "createInstance should throw if caller does not have permission to create wds-instance resource in Sam"
        );
        assertEquals(expectedSamExceptionCode, samException.getRawStatusCode(),
                "SamException from createInstance should have same status code as the thrown ApiException");
        List<UUID> allInstances = instanceService.listInstances(VERSION);
        assertFalse(allInstances.contains(instanceId), "should not have created the instances.");
    }

    private void doSamDeleteTest(UUID instanceId, int expectedSamExceptionCode) {
        // bypass Sam and create the instance directly in the db
        instanceDao.createSchema(instanceId);
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
