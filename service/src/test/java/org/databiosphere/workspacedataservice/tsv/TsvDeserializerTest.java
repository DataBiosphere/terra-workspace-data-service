package org.databiosphere.workspacedataservice.tsv;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

@SpringBootTest
public class TsvDeserializerTest {

    @Autowired
    TsvDeserializer tsvDeserializer;

   private static Stream<Arguments> fixtures() {
		/* Arguments are sets:
			- first value is the text that would be contained in a TSV cell
			- second value is the expected Java type that the TsvConverter would create for that cell
		 */
        return Stream.of(
                Arguments.of("[\"test1\", \"test2\"]", Arrays.asList("test1", "test2")),
                Arguments.of("[“test1”, “test2”]", Arrays.asList("test1", "test2")), // smart quotes
                Arguments.of("[\"98\", \"99\"]", Arrays.asList("98", "99")),
                Arguments.of("[true, false, true]", Arrays.asList(true, false, true)),
                Arguments.of(" ", " "),
                Arguments.of("5.67", BigDecimal.valueOf(5.67d))
        );
    }


    @ParameterizedTest(name = "TSV parsing for value <{0}> should result in <{1}>")
    @MethodSource("fixtures")
    void cellToAttributeTest(String input, Object expected) {
        Object actual = tsvDeserializer.cellToAttribute(input);

        if (expected instanceof List expectedList) {
            assertIterableEquals(expectedList, (List)actual);
        } else {
            assertEquals(expected, actual);
        }
    }

}
