package org.databiosphere.workspacedataservice.service;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"unit-test", "errorOnResourceCreateDelete"})
@SpringBootTest
class ErrorOnResourceCreateDeleteInstanceServiceUnsuccessfulSamTest extends AbstractInstanceServiceUnsuccessfulSamTest {

}
