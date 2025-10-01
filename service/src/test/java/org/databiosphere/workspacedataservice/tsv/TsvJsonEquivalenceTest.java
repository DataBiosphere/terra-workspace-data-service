package org.databiosphere.workspacedataservice.tsv;

import static org.databiosphere.workspacedataservice.service.model.ReservedNames.RECORD_ID;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import org.databiosphere.workspacedataservice.common.ControlPlaneTestBase;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Tests that, when supplied the same inputs, TSV uploads and JSON request payloads deserialize into
 * the same Java objects inside RecordAttributes.
 *
 * @see TsvJsonArgumentsProvider
 */
@SpringBootTest
class TsvJsonEquivalenceTest extends ControlPlaneTestBase {

  @Autowired private ObjectReader tsvReader;

  @Autowired private ObjectMapper mapper;

  private RecordAttributes readTsv(String tsvContent) throws IOException {
    InputStream inputStream = new ByteArrayInputStream(tsvContent.getBytes());
    InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
    MappingIterator<RecordAttributes> tsvIterator = tsvReader.readValues(inputStreamReader);
    // take the first row from the TSV, and remove its primary key
    RecordAttributes recordAttributes = tsvIterator.next();
    recordAttributes.removeAttribute(RECORD_ID);

    return recordAttributes;
  }

  /**
   * @see TsvJsonArgumentsProvider for arguments
   */
  @ParameterizedTest(
      name = "TSV and JSON deserialization should be equal for input value <{0}>, returning <{1}>")
  @ArgumentsSource(TsvJsonArgumentsProvider.class)
  void tsvAndJsonShouldDeserializeSame(String input, Object expected, boolean quoteJson)
      throws IOException {
    String tsv = RECORD_ID + "\tcol1\n123\t" + input + "\n";

    String jsonValue;
    if (quoteJson) {
      jsonValue = "\"" + input + "\"";
    } else {
      jsonValue = input;
    }

    String json =
        """
                {"attributes":{"col1":%s}}
                """
            .formatted(jsonValue);

    RecordAttributes tsvAttributes = readTsv(tsv);
    RecordAttributes jsonAttributes =
        mapper.readValue(json, RecordRequest.class).recordAttributes();

    Object tsvActual = tsvAttributes.getAttributeValue("col1");
    Object jsonActual = jsonAttributes.getAttributeValue("col1");

    if (tsvActual instanceof List<?> tsvList && jsonActual instanceof List<?> jsonList) {
      assertIterableEquals(jsonList, tsvList, "JSON and TSV arrays should be equal");
      assertIterableEquals((List<?>) expected, jsonList, "JSON array incorrect");
      assertIterableEquals((List<?>) expected, tsvList, "TSV array incorrect");
    } else if (expected == null) {
      assertNull(jsonActual, "JSON value should be null");
      assertNull(tsvActual, "TSV value should be null");
    } else {
      assertNotNull(jsonActual, "JSON value should not be null");
      assertNotNull(tsvActual, "TSV value should not be null");
      assertInstanceOf(jsonActual.getClass(), tsvActual, "JSON and TSV classes should be equal");
      assertEquals(expected, jsonActual, "JSON value incorrect");
      assertEquals(expected, tsvActual, "TSV value incorrect");
    }
  }
}
