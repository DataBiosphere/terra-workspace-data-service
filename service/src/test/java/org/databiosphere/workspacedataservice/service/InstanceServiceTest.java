package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.MockInstanceDaoConfig;
import org.databiosphere.workspacedataservice.sam.MockSamClientFactoryConfig;
import org.databiosphere.workspacedataservice.sam.SamConfig;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.service.model.exception.MissingObjectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.UUID;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ActiveProfiles(profiles = { "mock-sam", "mock-instance-dao" })
@SpringBootTest(classes = { MockInstanceDaoConfig.class, SamConfig.class, MockSamClientFactoryConfig.class })
@TestPropertySource(properties = {"twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000"}) // example uuid from https://en.wikipedia.org/wiki/Universally_unique_identifier
class InstanceServiceTest {

    private InstanceService instanceService;
    @Autowired private InstanceDao instanceDao;
    @Autowired private SamDao samDao;


    private static final UUID INSTANCE = UUID.fromString("111e9999-e89b-12d3-a456-426614174000");

    @BeforeEach
    void setUp() {
        instanceService = new InstanceService(instanceDao, samDao);
        // Delete all instances
        instanceDao.listInstanceSchemas().forEach(instance -> {
            instanceDao.dropSchema(instance);
        });
    }

    @Test
    void testCreateAndValidateInstance() {
        instanceService.createInstance(INSTANCE, VERSION);
        instanceService.validateInstance(INSTANCE);

        UUID invalidInstance = UUID.fromString("000e4444-e22b-22d1-a333-426614174000");
        assertThrows(MissingObjectException.class, () -> instanceService.validateInstance(invalidInstance),
            "validateInstance should have thrown an error");

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
        assertThrows(MissingObjectException.class, () -> instanceService.validateInstance(INSTANCE),
            "validateInstance should have thrown an error");
    }
}
