package org.databiosphere.workspacedataservice.pact;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.RequestResponsePact;
import au.com.dius.pact.core.model.annotations.Pact;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.StatusApi;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.databiosphere.workspacedataservice.sam.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.when;

@Tag("pact-test")
@ExtendWith(PactConsumerTestExt.class)
@ActiveProfiles(profiles = { "mock-sam" })
@SpringBootTest(classes = {SamConfig.class, MockSamClientFactoryConfig.class})
class SamPactTest {

    @Autowired
    SamDao samDao;

    @MockBean
    SamClientFactory mockSamClientFactory;

    // mock for the StatusApi class inside the Sam client; since this is not a Spring bean we have to mock it manually
    StatusApi mockStatusApi = Mockito.mock(StatusApi.class);

    ApiClient mockApiClient = Mockito.mock(ApiClient.class);
    @BeforeEach
    void beforeEach() {
        // return the mock StatusApi from the mock SamClientFactory
        given(mockSamClientFactory.getStatusApi()).willReturn(mockStatusApi);
        Mockito.clearInvocations(mockStatusApi);
    }

    @Pact(consumer = "wds-consumer", provider = "sam-provider")
    public RequestResponsePact statusApiPact(PactDslWithProvider builder) {
        return builder
                .given("Sam is ok")
                .uponReceiving("a status request")
                .path("/status")
                .method("GET")
                .willRespondWith()
                .status(200)
                .body("{\"ok\": true}")
                .toPact();
    }

    @Test
    @PactTestFor(pactMethod = "statusApiPact")
    public void testSamServiceStatusCheck(MockServer mockServer) {
        given(mockSamClientFactory.getStatusApi()).willReturn(mockStatusApi);
        given(mockStatusApi.getApiClient()).willReturn(mockApiClient);
        given(mockApiClient.getBasePath()).willReturn(mockServer.getUrl());

        SystemStatus samStatus = samDao.getSystemStatus();
        assertTrue(samStatus.getOk());
    }
}
