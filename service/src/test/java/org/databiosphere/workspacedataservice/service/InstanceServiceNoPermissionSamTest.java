package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.*;

@ActiveProfiles({"unit-test", "errorOnPermissionCheck"})
@SpringBootTest
class InstanceServiceNoPermissionSamTest {

    @Autowired
    private InstanceService instanceService;

    @Test
    void testCreateInstanceNoPermission() {
        // see MockSamResourcesApi for an explanation of how the instanceId will trigger various errors/permission failures.
        // since this UUID starts with 9, our mock will return a success from Sam containing a boolean false
        UUID instanceId = UUID.fromString("90000000-0000-0000-0000-000000000000");
        assertThrows(AuthorizationException.class,
                () -> instanceService.createInstance(instanceId, VERSION, Optional.empty()),
                "createInstance should throw if caller does not have permission to create wds-instance resource in Sam"
        );
        List<UUID> allInstances = instanceService.listInstances(VERSION);
        assertFalse(allInstances.contains(instanceId), "should not have created the instances.");
    }
}
