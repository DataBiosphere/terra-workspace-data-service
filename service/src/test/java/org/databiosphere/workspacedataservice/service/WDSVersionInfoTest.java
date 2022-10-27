package org.databiosphere.workspacedataservice.service;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.FileNotFoundException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;


@SpringBootTest
class WDSVersionInfoTest {

    @Test
    void testGetWDSVersionInfo() throws FileNotFoundException {
        Map<String, String> versionDetailsMap = new WDSVersionInfo().getWDSVersionInfo();
        assertTrue(versionDetailsMap.containsKey("gitShortSHA"));
        assertTrue(versionDetailsMap.containsKey("semanticVersion"));
    }

}
