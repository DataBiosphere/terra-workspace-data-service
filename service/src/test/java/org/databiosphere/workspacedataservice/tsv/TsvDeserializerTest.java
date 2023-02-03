package org.databiosphere.workspacedataservice.tsv;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigInteger;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that TSV uploads deserialize into the expected  Java objects inside RecordAttributes.
 *
 * @see TsvJsonArgumentsProvider
 */
@SpringBootTest
public class TsvDeserializerTest {

    @Autowired
    TsvDeserializer tsvDeserializer;

    // ===== nodeToObject tests:
    @Test
    void nullNodeInput() {
        JsonNode input = NullNode.getInstance();
        Object actual = tsvDeserializer.nodeToObject(input);
        assertNull(actual, "nodeToObject should return null for NullNode input");
    }

    @Test
    void nonTextNodeInput() {
        JsonNode input = BigIntegerNode.valueOf(BigInteger.valueOf(12345));
        Object actual = tsvDeserializer.nodeToObject(input);
        assertEquals("12345", actual, "nodeToObject should return string representations for non-TextNode");
    }

    @Test
    void textNodeInput() {
        String fixture = "lorem ipsum";
        JsonNode input = TextNode.valueOf(fixture);
        Object actual = tsvDeserializer.nodeToObject(input);
        assertEquals(fixture, actual, "nodeToObject should return value for TextNode");
    }

    // ===== cellToAttribute tests:



    /**
     * @see TsvJsonArgumentsProvider for arguments
     */
    @ParameterizedTest(name = "cellToAttribute for input value <{0}> should return <{1}>")
    @ArgumentsSource(TsvJsonArgumentsProvider.class)
    void cellToAttributeTest(String input, Object expected) {
        Object actual = tsvDeserializer.cellToAttribute(input);

        //noinspection rawtypes
        if (expected instanceof List expectedList) {
            //noinspection rawtypes
            assertIterableEquals(expectedList, (List) actual, "cellToAttribute for input value <%s> should return <%s>".formatted(input, expected));
        } else {
            assertEquals(expected, actual, "cellToAttribute for input value <%s> should return <%s>".formatted(input, expected));
        }
    }

    @ParameterizedTest(name = "cellToAttribute for input value <{0}> should return <{1}>")
    @ArgumentsSource(TsvOnlyArgumentsProvider.class)
    void tsvOnlyCases(String input, Object expected) {
        Object actual = tsvDeserializer.cellToAttribute(input);

        //noinspection rawtypes
        if (expected instanceof List expectedList) {
            //noinspection rawtypes
            assertIterableEquals(expectedList, (List)actual, "cellToAttribute for input value <%s> should return <%s>".formatted(input, expected));
        } else {
            assertEquals(expected, actual, "cellToAttribute for input value <%s> should return <%s>".formatted(input, expected));
        }
    }

}
