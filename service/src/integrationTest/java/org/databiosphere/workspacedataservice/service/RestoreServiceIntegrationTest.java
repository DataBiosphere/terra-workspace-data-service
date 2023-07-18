package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.dao.InstanceDao;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.shared.model.job.JobStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;

@ActiveProfiles({"mock-storage", "local"})
@ContextConfiguration(name = "mockStorage")
@DirtiesContext
@SpringBootTest
@TestPropertySource(
        properties = {
                "twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000",
                "twds.instance.source-workspace-id=123e4567-e89b-12d3-a456-426614174001",
                "twds.pg_dump.useAzureIdentity=false"
})
public class RestoreServiceIntegrationTest {
    @Autowired
    private BackupRestoreService backupRestoreService;

    @Autowired
    InstanceDao instanceDao;

    @Autowired
    RecordDao recordDao;

    @Value("${twds.instance.workspace-id:}")
    private String workspaceId;

    @Value("${twds.instance.source-workspace-id:}")
    private String sourceWorkspaceId;

    @BeforeEach
    void beforeEach() {
        // clean up any instances left in the db
        List<UUID> allInstances = instanceDao.listInstanceSchemas();
        allInstances.forEach(instanceId -> instanceDao.dropSchema(instanceId));
    }

    // this test references the file src/integrationTest/resources/backup-test.sql as its backup
    @Test
    void testRestoreAzureWDS() {
        UUID sourceInstance = UUID.fromString(sourceWorkspaceId);
        UUID destInstance = UUID.fromString(workspaceId);

        // confirm neither source nor destination instance should exist in our list of schemas to start
        List<UUID> instancesBefore = instanceDao.listInstanceSchemas();
        assertThat(instancesBefore).doesNotContain(destInstance);
        assertThat(instancesBefore).doesNotContain(sourceInstance);

        // perform the restore
        var response = backupRestoreService.restoreAzureWDS("v0.2", "backup.sql", UUID.randomUUID(), "");
        assertSame(JobStatus.SUCCEEDED, response.getStatus());

        instanceDao.dropInstanceFromSyswds(sourceInstance);

        // after restore, confirm destination instance exists but source does not
        List<UUID> instancesAfter = instanceDao.listInstanceSchemas();
        assertThat(instancesAfter).contains(destInstance);
        assertThat(instancesAfter).doesNotContain(sourceInstance);

        // after restore, target instance should have one table named "test"
        List<RecordType> tables = recordDao.getAllRecordTypes(destInstance);
        assertThat(tables.size()).isEqualTo(1);
        assertThat(tables).contains(RecordType.valueOf("test"));
    }
}