package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
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

@ActiveProfiles(profiles = "unit-test")
@SpringBootTest
class InstanceServiceTest {

    @Autowired private InstanceService instanceService;
    @Autowired private RecordDao recordDao;


    private static final UUID INSTANCE = UUID.fromString("111e9999-e89b-12d3-a456-426614174000");

    @BeforeEach
    void setUp() {
        // Delete all instances
        recordDao.listInstanceSchemas().forEach(instance -> {
            recordDao.dropSchema(instance);
        });
    }

    @Test
    void testCreateAndValidateInstance() {
        instanceService.createInstance(INSTANCE, VERSION, Optional.empty());
        instanceService.validateInstance(INSTANCE);

        UUID invalidInstance = UUID.fromString("000e4444-e22b-22d1-a333-426614174000");
        assertThrows(MissingObjectException.class, () -> instanceService.validateInstance(invalidInstance),
            "validateInstance should have thrown an error");

        // clean up
        instanceService.deleteInstance(INSTANCE, VERSION);
    }

    @Test
    void listInstances() {
        instanceService.createInstance(INSTANCE, VERSION, Optional.empty());

        UUID secondInstanceId = UUID.fromString("999e1111-e89b-12d3-a456-426614174000");
        instanceService.createInstance(secondInstanceId, VERSION, Optional.empty());

        List<UUID> instances = instanceService.listInstances(VERSION);

        assertEquals(2, instances.size());
        assert(instances.contains(INSTANCE));
        assert(instances.contains(secondInstanceId));

        instanceService.deleteInstance(INSTANCE, VERSION);
        instanceService.deleteInstance(secondInstanceId, VERSION);
    }

    @Test
    void deleteInstance() {
        instanceService.createInstance(INSTANCE, VERSION, Optional.empty());
        instanceService.validateInstance(INSTANCE);

        instanceService.deleteInstance(INSTANCE, VERSION);
        assertThrows(MissingObjectException.class, () -> instanceService.validateInstance(INSTANCE),
            "validateInstance should have thrown an error");
    }
}
