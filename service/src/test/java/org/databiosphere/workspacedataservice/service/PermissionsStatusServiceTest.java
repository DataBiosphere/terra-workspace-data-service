package org.databiosphere.workspacedataservice.service;

import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.databiosphere.workspacedataservice.sam.HttpSamDao;
import org.databiosphere.workspacedataservice.sam.PermissionsStatusService;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.mockito.exceptions.base.MockitoException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(properties = "spring.cache.type=NONE")
@TestPropertySource(properties = {"twds.instance.workspace-id=123e4567-e89b-12d3-a456-426614174000"}) // example uuid from https://en.wikipedia.org/wiki/Universally_unique_identifier
class PermissionsStatusServiceTest {

    @Autowired
    private PermissionsStatusService samStatusService;

    @MockBean
    HttpSamDao httpSamDao;

    @MockBean
    SamClientFactory mockSamClientFactory;

    // mock for the StatusApi class inside the Sam client; since this is not a Spring bean we have to mock it manually
    final StatusApi mockStatusApi = Mockito.mock(StatusApi.class);
    // mock for Health.Builder class that Spring Boot Actuator relies on to determine overall health of an application.
    final Health.Builder mockHealthBuilder = Mockito.mock(Health.Builder.class);

    @BeforeEach
    void beforeEach() {
        // return the mock StatusApi from the mock SamClientFactory
        given(mockSamClientFactory.getStatusApi()).willReturn(mockStatusApi);
        Mockito.clearInvocations(mockStatusApi);
        Mockito.clearInvocations(mockHealthBuilder);
    }

    @ParameterizedTest(name = "SAM Status to successfully invoke builder when {0}")
    @ValueSource(booleans = {false, true})
    void testSamHealthCall(boolean isHealthy) throws Exception {
        SystemStatus status = new SystemStatus();
        status.ok(isHealthy);
        when(mockStatusApi.getSystemStatus()).thenReturn(status);
        samStatusService.doHealthCheck(mockHealthBuilder);
        verify(mockStatusApi, times(1)).getSystemStatus();
    }

    @Test
    public void testSamExceptionUnhealthyCall() throws Exception {
        when(mockStatusApi.getSystemStatus()).thenThrow(new MockitoException("Hey SAM is down!"));
        samStatusService.doHealthCheck(mockHealthBuilder);
        verify(mockHealthBuilder, times(1)).withDetail("samConnectionError", "500 INTERNAL_SERVER_ERROR \"Error from Sam: Hey SAM is down!\"");
    }
}
