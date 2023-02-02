package org.databiosphere.workspacedataservice.tsv;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that, when supplied the same inputs, TSV uploads and JSON request payloads
 * deserialize into the same Java objects inside RecordAttributes.
 *
 * @see TsvJsonArgumentsProvider
 */
@SpringBootTest
public class TsvJsonEquivalenceTest {

    @Autowired
    private ObjectReader tsvReader;

    @Autowired
    private ObjectMapper mapper;

    private RecordAttributes readTsv(String tsvContent) throws IOException {
        InputStream inputStream = new ByteArrayInputStream(tsvContent.getBytes());
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        MappingIterator<RecordAttributes> tsvIterator = tsvReader.readValues(inputStreamReader);
        // take the first row from the TSV, and remove its primary key
        RecordAttributes recordAttributes = tsvIterator.next();
        recordAttributes.remove(RECORD_ID);

        return recordAttributes;
    }


    /**
     * @see TsvJsonArgumentsProvider for arguments
     */
    @ParameterizedTest(name = "TSV and JSON deserialization should be equal for input value <{0}>, returning <{1}>")
    @ArgumentsSource(TsvJsonArgumentsProvider.class)
    void tsvAndJsonShouldDeserializeSame(String input, Object expected, boolean quoteJson) throws IOException {
        String tsv = RECORD_ID + "\tcol1\n123\t" + input + "\n";

        String jsonValue;
        if (quoteJson) {
            jsonValue = "\"" + input + "\"";
        } else {
            jsonValue = input;
        }

        String json = """
                {"attributes":{"col1":%s}}
                """.formatted(jsonValue);

        RecordAttributes tsvAttributes = readTsv(tsv);
        RecordAttributes jsonAttributes = mapper.readValue(json, RecordRequest.class).recordAttributes();

        Object tsvActual = tsvAttributes.getAttributeValue("col1");
        Object jsonActual = jsonAttributes.getAttributeValue("col1");

        if (tsvActual instanceof List tsvList && jsonActual instanceof List jsonList) {
            assertIterableEquals(tsvList, jsonList);
        } else {
            assertInstanceOf(jsonActual.getClass(), tsvActual);
            assertEquals(expected, jsonActual);
            assertEquals(expected, tsvActual);
        }

    }



//
//    @ParameterizedTest(name = "Scalar deserialization for input <{0}> should be equivalent for TSV and JSON")
//    @ValueSource(strings = {"hello world", "terra-wds:/targetType/targetId",
//            "2021-10-03", "2021-10-03T19:01:23"})
//    void scalarDeserializationQuotedJson(String input) throws IOException {
//        String tsv = RECORD_ID + "\tcol1\n123\t" + input + "\n";
//        String json = """
//                {"attributes":{"col1":"%s"}}
//                """.formatted(input);
//
//        RecordAttributes jsonAttributes = mapper.readValue(json, RecordRequest.class).recordAttributes();
//
//        RecordAttributes tsvAttributes = readTsv(tsv);
//
//        assertInstanceOf(jsonAttributes.getAttributeValue("col1").getClass(), tsvAttributes.getAttributeValue("col1"));
//
//        assertEquals(jsonAttributes, tsvAttributes);
//    }
//
//    @ParameterizedTest(name = "Scalar deserialization for input <{0}> should be equivalent for TSV and JSON")
//    @ValueSource(strings = {"123.45", "789", "true", "{\"foo\":\"bar\",\"num\":123}"})
//    void scalarDeserializationUnquotedJson(String input) throws IOException {
//        String tsv = RECORD_ID + "\tcol1\n123\t" + input + "\n";
//        String json = """
//                {"attributes":{"col1":%s}}
//                """.formatted(input);
//
//        RecordAttributes jsonAttributes = mapper.readValue(json, RecordRequest.class).recordAttributes();
//
//        RecordAttributes tsvAttributes = readTsv(tsv);
//
//        assertInstanceOf(jsonAttributes.getAttributeValue("col1").getClass(), tsvAttributes.getAttributeValue("col1"));
//
//        assertEquals(jsonAttributes, tsvAttributes);
//    }
//
//    @ParameterizedTest(name = "Array deserialization for input <{0}> should be equivalent for TSV and JSON")
//    @ValueSource(strings = {"[\"hello\",\"world\"]", "[12,34,56]", "[true, false, true]"})
//    void arrayDeserialization(String input) throws IOException {
//        String tsv = RECORD_ID + "\tcol1\n123\t" + input + "\n";
//        String json = """
//                {"attributes":{"col1":%s}}
//                """.formatted(input);
//
//        RecordAttributes jsonAttributes = mapper.readValue(json, RecordRequest.class).recordAttributes();
//
//        RecordAttributes tsvAttributes = readTsv(tsv);
//
//        assertInstanceOf(List.class, jsonAttributes.getAttributeValue("col1"));
//        assertInstanceOf(List.class, tsvAttributes.getAttributeValue("col1"));
//
//        List jsonList = (List)jsonAttributes.getAttributeValue("col1");
//        List tsvList = (List)tsvAttributes.getAttributeValue("col1");
//
//        assertIterableEquals(jsonList, tsvList);
//    }

}
