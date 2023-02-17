package org.databiosphere.workspacedataservice.service;

import org.aspectj.lang.annotation.Before;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.*;

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
        instanceService.createInstance(INSTANCE, VERSION);
        instanceService.validateInstance(INSTANCE);

        try {
            instanceService.validateInstance(UUID.fromString("000e4444-e22b-22d1-a333-426614174000"));
            // Test should not reach this line
            assert(false);
        } catch (MissingObjectException e) {
            // This is expected
        }
        // clean up
        instanceService.deleteInstance(INSTANCE, VERSION);
    }

    @Test
    void listInstances() {
        instanceService.createInstance(INSTANCE, VERSION);

        UUID secondInstanceId = UUID.fromString("999e1111-e89b-12d3-a456-426614174000");
        instanceService.createInstance(secondInstanceId, VERSION);

        List<UUID> instances = instanceService.listInstances(VERSION);

        assertEquals(2, instances.size());
        assert(instances.contains(INSTANCE));
        assert(instances.contains(secondInstanceId));

        instanceService.deleteInstance(INSTANCE, VERSION);
        instanceService.deleteInstance(secondInstanceId, VERSION);
    }

    @Test
    void deleteInstance() {
        instanceService.createInstance(INSTANCE, VERSION);
        instanceService.validateInstance(INSTANCE);

        instanceService.deleteInstance(INSTANCE, VERSION);
        try {
            instanceService.validateInstance(INSTANCE);

            // Test should not reach this line
            assert(false);
        } catch (MissingObjectException e) {
            // This is expected
        }
    }
}
