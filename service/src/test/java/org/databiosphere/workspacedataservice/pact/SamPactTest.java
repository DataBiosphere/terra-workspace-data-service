package org.databiosphere.workspacedataservice.pact;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.RequestResponsePact;
import au.com.dius.pact.core.model.annotations.Pact;
import org.broadinstitute.dsde.workbench.client.sam.model.SystemStatus;
import org.databiosphere.workspacedataservice.sam.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("pact-test")
@ExtendWith(PactConsumerTestExt.class)
class SamPactTest {

    @BeforeAll
    static void setup() {
        //Without this setup, the HttpClient throws a "No thread-bound request found" error
        MockHttpServletRequest request = new MockHttpServletRequest();
        // Set the mock request as the current request context
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
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
    void testSamServiceStatusCheck(MockServer mockServer) {
        SamClientFactory clientFactory = new HttpSamClientFactory(mockServer.getUrl());
        SamDao samDao = new HttpSamDao(clientFactory, new HttpSamClientSupport(), UUID.randomUUID().toString());

        SystemStatus samStatus = samDao.getSystemStatus();
        assertTrue(samStatus.getOk());
    }
}
