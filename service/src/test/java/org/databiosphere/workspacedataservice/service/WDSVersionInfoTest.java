package org.databiosphere.workspacedataservice.service;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;


@SpringBootTest
class WDSVersionInfoTest {

    @Test
    void testGetWDSVersionInfo() throws IOException {
        Map<String, String> versionDetailsMap = new WDSVersionInfo().getWDSVersionInfo();
        assertTrue(versionDetailsMap.containsKey("gitShortSHA"));
        assertTrue(versionDetailsMap.containsKey("semanticVersion"));
    }

}
