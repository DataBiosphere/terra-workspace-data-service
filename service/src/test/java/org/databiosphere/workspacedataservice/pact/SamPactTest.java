package org.databiosphere.workspacedataservice.pact;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.RequestResponsePact;
import au.com.dius.pact.core.model.annotations.Pact;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.databiosphere.workspacedataservice.sam.HttpSamDao;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.mock.mockito.MockBean;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("pact-test")
@ExtendWith(PactConsumerTestExt.class)
class SamPactTest {

    @MockBean
    HttpSamDao samDao;

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
        SystemStatus samStatus = samDao.getSystemStatus();
        assertTrue(samStatus.getOk());
    }
}
