package org.databiosphere.workspacedataservice.service;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"unit-test", "errorOnPermissionCheck"})
@SpringBootTest
class ErrorOnPermissionCheckInstanceServiceUnsuccessfulSamTest extends AbstractInstanceServiceUnsuccessfulSamTest {

}
