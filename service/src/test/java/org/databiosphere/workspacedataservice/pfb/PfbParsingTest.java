package org.databiosphere.workspacedataservice.pfb;

import static org.springframework.test.util.AssertionErrors.assertEquals;

import bio.terra.pfb.Library;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;


@DirtiesContext
@SpringBootTest
class PfbParsingTest {

    @Test
    void testPFBHelloWorld() {
        int valReturned = Library.getNumber5();
        assertEquals("PFB Hello World", valReturned, 5);
    }
}
