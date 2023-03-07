package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.SamException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
abstract class AbstractInstanceServiceUnsuccessfulSamTest {

    @Autowired private InstanceService instanceService;

    private static Stream<Arguments> provideExceptionTriggers() {
		/* Arguments are sets:
			- first value is the first letter of the instanceId, which will trigger exceptions in MockSamResourcesApi.maybeThrow()
			- second value is the expected status code that createInstance will throw in response to the Sam exception

			see MockSamResourcesApi.maybeThrow()
		 */
        return Stream.of(
                Arguments.of("c", 404),
                Arguments.of("d", 500),
                Arguments.of("e", 500),
                Arguments.of("f", 500)
        );
    }

    @ParameterizedTest(name = "createInstance should throw AuthorizationException if the Sam call throws an exception ({0})")
    @ValueSource(strings = {"a", "b"})
    void testCreateInstanceAuthorizationException(String firstLetterOfUuid) {
        // see MockSamResourcesApi for an explanation of how the instanceId will trigger various errors/permission failures
        UUID instanceId = UUID.fromString(firstLetterOfUuid + "0000000-0000-0000-0000-000000000000");
        assertThrows(AuthorizationException.class,
                () -> instanceService.createInstance(instanceId, VERSION, Optional.empty()),
                "createInstance should throw if caller does not have permission to create wds-instance resource in Sam"
        );
        List<UUID> allInstances = instanceService.listInstances(VERSION);
        assertFalse(allInstances.contains(instanceId), "should not have created the instances.");
    }

    @ParameterizedTest(name = "createInstance should throw {1} if the Sam call throws an exception ({0})")
    @MethodSource("provideExceptionTriggers")
    void testCreateInstanceSamException(String firstLetterOfUuid, int expectedStatusCode) {
        // see MockSamResourcesApi for an explanation of how the instanceId will trigger various errors/permission failures
        UUID instanceId = UUID.fromString(firstLetterOfUuid + "0000000-0000-0000-0000-000000000000");
        SamException samException = assertThrows(SamException.class,
                () -> instanceService.createInstance(instanceId, VERSION, Optional.empty()),
                "createInstance should throw if caller does not have permission to create wds-instance resource in Sam"
        );
        assertEquals(expectedStatusCode, samException.getRawStatusCode(), "expected correct http status code");
        List<UUID> allInstances = instanceService.listInstances(VERSION);
        assertFalse(allInstances.contains(instanceId), "should not have created the instances.");
    }


//    @Test
//    void deleteInstance() {
//        instanceService.createInstance(INSTANCE, VERSION, Optional.empty());
//        instanceService.validateInstance(INSTANCE);
//
//        instanceService.deleteInstance(INSTANCE, VERSION);
//        assertThrows(MissingObjectException.class, () -> instanceService.validateInstance(INSTANCE),
//            "validateInstance should have thrown an error");
//    }
}
