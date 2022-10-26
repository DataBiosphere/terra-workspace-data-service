package org.databiosphere.workspacedataservice;

import org.databiosphere.workspacedata.api.InstancesApi;
import org.databiosphere.workspacedata.client.ApiClient;
import org.databiosphere.workspacedata.client.ApiException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;

import java.util.UUID;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class SwaggerTest {

    @LocalServerPort
    int port;

    @Test
    void validateThatImplementationMatchesDocumentationSpecification() throws ApiException {
        ApiClient client = new ApiClient();
        client.setBasePath("http://localhost:"+port);
        InstancesApi instancesApi = new InstancesApi(client);
        String instanceid = UUID.randomUUID().toString();
        instancesApi.createWDSInstance(instanceid, "v0.2");
    }
}
