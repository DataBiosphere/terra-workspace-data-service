package org.databiosphere.workspacedataservice.service;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StatusServiceTest {

    @Autowired
    private SamDao samDao;

    @Autowired
    private StatusService statusService;

    @MockBean
    SamClientFactory mockSamClientFactory;

    // mock for the StatusApi class inside the Sam client; since this is not a Spring bean we have to mock it manually
    StatusApi mockStatusApi = Mockito.mock(StatusApi.class);

    @BeforeEach
    void beforeEach() throws ApiException {
        // return the mock StatusApi from the mock SamClientFactory
        given(mockSamClientFactory.getStatusApi()).willReturn(mockStatusApi);
        Mockito.clearInvocations(mockStatusApi);
    }

    @Test
    void checkStatusSamCalls() throws Exception {
        SystemStatus status = new SystemStatus();
        status.ok(true);
        when(mockStatusApi.getSystemStatus()).thenReturn(status);
        Health.Builder builder = new Health.Builder();
        statusService.doHealthCheck(builder);
        verify(mockStatusApi, times(1)).getSystemStatus();
    }
}
