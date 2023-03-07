package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.sam.MockSamResourcesApi;
import org.databiosphere.workspacedataservice.sam.SamClientFactory;
import org.databiosphere.workspacedataservice.service.model.exception.AuthorizationException;
import org.databiosphere.workspacedataservice.service.model.exception.SamException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.databiosphere.workspacedataservice.service.RecordUtils.VERSION;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.BDDMockito.given;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ErrorOnResourceCreateDeleteInstanceServiceUnsuccessfulSamTest extends AbstractInstanceServiceUnsuccessfulSamTest {
    @MockBean
    SamClientFactory samClientFactory;

    @BeforeEach
    void beforeEach() {
        given(samClientFactory.getResourcesApi())
                .willReturn(new MockSamResourcesApi(false, true));
    }

}
