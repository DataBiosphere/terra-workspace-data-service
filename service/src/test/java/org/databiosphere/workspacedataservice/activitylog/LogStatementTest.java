package org.databiosphere.workspacedataservice.activitylog;

import org.databiosphere.workspacedataservice.dao.MockInstanceDaoConfig;
import org.databiosphere.workspacedataservice.datarepo.DataRepoConfig;
import org.databiosphere.workspacedataservice.sam.MockSamClientFactoryConfig;
import org.databiosphere.workspacedataservice.sam.SamConfig;
import org.databiosphere.workspacedataservice.service.DataRepoService;
import org.databiosphere.workspacedataservice.service.InstanceService;
import org.databiosphere.workspacedataservice.workspacemanager.WorkspaceManagerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@ActiveProfiles(profiles = { "mock-sam", "mock-instance-dao" })
@SpringBootTest(classes = {ActivityLoggerConfig.class,
        DataRepoService.class, DataRepoConfig.class,
        WorkspaceManagerConfig.class,
        MockSamClientFactoryConfig.class, SamConfig.class,
        InstanceService.class, MockInstanceDaoConfig.class
})
@ExtendWith(OutputCaptureExtension.class)
public class LogStatementTest {

    private final String VERSION = "v0.2";

    @Autowired
    InstanceService instanceService;

    @Test
    void createInstanceLogging(CapturedOutput output) {
        UUID instanceId = UUID.randomUUID();
        instanceService.createInstance(instanceId, VERSION);
        assertThat(output.getOut())
                .contains("user anonymous created 1 instance(s) with id(s) [%s]".formatted(instanceId));
    }

    @Test
    void deleteInstanceLogging(CapturedOutput output) {
        UUID instanceId = UUID.randomUUID();
        instanceService.createInstance(instanceId, VERSION);
        instanceService.deleteInstance(instanceId, VERSION);
        assertThat(output.getOut())
                .contains("user anonymous deleted 1 instance(s) with id(s) [%s]".formatted(instanceId));
    }


}
