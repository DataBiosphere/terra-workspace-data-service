package org.databiosphere.workspacedataservice.leo;

import org.broadinstitute.dsde.workbench.client.leonardo.api.AppsV2Api;
import org.databiosphere.workspacedataservice.leonardo.LeonardoClientFactory;
import org.databiosphere.workspacedataservice.leonardo.LeonardoConfig;
import org.databiosphere.workspacedataservice.leonardo.LeonardoDao;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;

@DirtiesContext
@SpringBootTest(classes = {LeonardoConfig.class})
public class LeonardoDaoTest {
    @Autowired
    LeonardoDao leonardoDao;

    @MockBean
    LeonardoClientFactory leonardoClientFactory;

    final AppsV2Api mockAppsApi = Mockito.mock(AppsV2Api.class);
}

