package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.sam.MockSamResourcesApi;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import static org.mockito.BDDMockito.given;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ErrorOnPermissionCheckInstanceServiceUnsuccessfulSamTest extends AbstractInstanceServiceUnsuccessfulSamTest {

    @MockBean
    SamClientFactory samClientFactory;

    @BeforeEach
    void beforeEach() {
        given(samClientFactory.getResourcesApi())
                .willReturn(new MockSamResourcesApi(true, false));
    }



}
