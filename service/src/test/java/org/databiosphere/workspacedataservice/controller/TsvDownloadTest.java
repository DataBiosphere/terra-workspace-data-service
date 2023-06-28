package org.databiosphere.workspacedataservice.controller;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.RandomStringUtils;
import org.databiosphere.workspacedataservice.dao.RecordDao;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.BatchResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordRequest;
import org.databiosphere.workspacedataservice.shared.model.RecordResponse;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.*;

@ActiveProfiles(profiles = "mock-sam")
@DirtiesContext
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TsvDownloadTest {

	@Autowired
	private TestRestTemplate restTemplate;

	@Autowired
	private RecordController recordController;
	@Autowired
	private RecordDao recordDao;
	private String version;
	private UUID instanceId;


	@BeforeEach
	void init(){
		version = "v0.2";
		instanceId = UUID.randomUUID();
		recordController.createInstance(instanceId, version);
	}

	@AfterEach
	void afterEach() {
		recordController.deleteInstance(instanceId, version);
	}

	@ParameterizedTest(name = "PK name {0} should be honored")
	@ValueSource(strings = {"Alfalfa", "buckWheat", "boo-yah", "sample id", "sample_id", "buttHead", "may 12 sample"})
	void tsvUploadWithChoosenPrimaryKeyFollowedByDownload(String primaryKeyName) throws IOException {
		MockMultipartFile file = new MockMultipartFile("records", "simple.tsv", MediaType.TEXT_PLAIN_VALUE,
				("col_1\tcol_2\t" + primaryKeyName + "\n" + "Fido\tJerry\t" + primaryKeyName + "_val\n").getBytes());
		String recordType = primaryKeyName+"_rt";
		recordController.tsvUpload(instanceId, version, RecordType.valueOf(recordType), Optional.of(primaryKeyName), file);
		HttpHeaders headers = new HttpHeaders();
		ResponseEntity<Resource> stream = restTemplate.exchange("/{instanceId}/tsv/{version}/{recordType}",
				HttpMethod.GET, new HttpEntity<>(headers), Resource.class, instanceId, version, recordType);
		InputStream inputStream = stream.getBody().getInputStream();
		CSVFormat csvFormat = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setDelimiter("\t")
				.setQuoteMode(QuoteMode.MINIMAL).build();
		InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
		Iterable<CSVRecord> records = new CSVParser(reader, csvFormat);
		Iterator<CSVRecord> iterator = records.iterator();
		CSVRecord rcd = iterator.next();
		assertThat(rcd.get(primaryKeyName)).isEqualTo(primaryKeyName+"_val");
		assertThat(rcd.get("col_1")).isEqualTo("Fido");
		assertThat(rcd.get("col_2")).isEqualTo("Jerry");
		assertThat(iterator.hasNext()).isFalse();
		reader.close();
	}
	@Test
	void batchWriteFollowedByTsvDownload() throws IOException {
		RecordType recordType = RecordType.valueOf("tsv-test");

		InputStream is = TsvDownloadTest.class.getResourceAsStream("/batch_write_tsv_data.json");
		ResponseEntity<BatchResponse> response = recordController.streamingWrite(instanceId, version, recordType, Optional.empty(), is);
		assertThat(response.getStatusCodeValue()).isEqualTo(200);
		assertThat(response.getBody().recordsModified()).isEqualTo(2);
		HttpHeaders headers = new HttpHeaders();
		ResponseEntity<Resource> stream = restTemplate.exchange("/{instanceId}/tsv/{version}/{recordType}",
				HttpMethod.GET, new HttpEntity<>(headers), Resource.class, instanceId, version, recordType);
		InputStream inputStream = stream.getBody().getInputStream();
		CSVFormat csvFormat = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setDelimiter("\t")
				.setQuoteMode(QuoteMode.MINIMAL).build();
		InputStreamReader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
		Iterable<CSVRecord> records = new CSVParser(reader, csvFormat);
		Iterator<CSVRecord> iterator = records.iterator();
		CSVRecord rcd = iterator.next();
		assertThat(rcd.get("description")).isEqualTo("Embedded\tTab");
		rcd = iterator.next();
		assertThat(rcd.get("description")).isEqualTo("\n,Weird\n String");
		assertThat(rcd.get("location")).isEqualTo("Cambridge, \"MA\"");
		assertThat(rcd.get("unicodeData")).isEqualTo("\uD83D\uDCA9ȇ");
		assertThat(iterator.hasNext()).isFalse();
		reader.close();
	}


	private static Stream<Arguments> tsvDownloadValues() {
        /* Arguments are sets:
            - first value is the Object to insert as an attribute
            - second value is the expected data type that the object creates
            - third value is the text that would be contained in a TSV cell when downloading the attribute
         */
		return Stream.of(
				Arguments.of(Boolean.TRUE,
						BOOLEAN, "true"),
				Arguments.of(Boolean.FALSE,
						BOOLEAN, "false"),
				Arguments.of("hello",
						STRING, "hello"),
				Arguments.of("embedded\ttab",
						STRING, "\"embedded\ttab\""),
				Arguments.of(BigDecimal.valueOf(789),
						NUMBER, "789"),
				Arguments.of("{\"foo\": \"bar\", \"arr\": [2,4,6]}",
						JSON, "\"{\"\"arr\"\":[2,4,6],\"\"foo\"\":\"\"bar\"\"}\""),
				Arguments.of(List.of("foo", "bar", "baz"),
						ARRAY_OF_STRING, "\"[\"\"foo\"\",\"\"bar\"\",\"\"baz\"\"]\""),
				Arguments.of(List.of(BigDecimal.valueOf(1), BigDecimal.valueOf(3), BigDecimal.valueOf(5)),
						ARRAY_OF_NUMBER, "[1,3,5]")


//				Arguments.of("2021-10-03",          "2021-10-03",           true),
//				Arguments.of("2021-10-03T19:01:23", "2021-10-03T19:01:23",  true),
//				Arguments.of("terra-wds:/type/id",  "terra-wds:/type/id",   true),
//				Arguments.of("[]", Collections.EMPTY_LIST, false),
//
//				// arrays of booleans
//				Arguments.of("[true,false,true]",       List.of(true, false, true),     false),

//				Arguments.of("[\"2021-10-03\", \"2022-11-04\"]",                    List.of("2021-10-03", "2022-11-04"),                    false),
//				Arguments.of("[\"2021-10-03T19:01:23\", \"2021-11-04T20:02:24\"]",  List.of("2021-10-03T19:01:23", "2021-11-04T20:02:24"),  false),
//				Arguments.of("[\"terra-wds:/type/id\", \"terra-wds:/type/id2\"]",   List.of("terra-wds:/type/id", "terra-wds:/type/id2"),   false),
//
//				// mixed array (these deserialize as mixed lists, will be coerced to a single data type later in processing)
//				Arguments.of("[\"hello\", 123, true]",  List.of("hello", BigInteger.valueOf(123), true),    false),


		);
	}


	@ParameterizedTest(name = "TSV download for a {1} should be correct")
	@MethodSource("tsvDownloadValues")
	void tsvDownloadVariousDataTypes(Object toUpload, DataTypeMapping expectedDataType, String expectedCellValue) throws IOException {
		RecordType recordType = RecordType.valueOf(RandomStringUtils.randomAlphabetic(8));
		String recordId = "123";
		String attrName = RandomStringUtils.randomAlphabetic(8);

		// upload a record containing the attribute in question
		RecordAttributes uploadAttrs = new RecordAttributes(Map.of(attrName, toUpload));
		RecordRequest recordRequest = new RecordRequest(uploadAttrs);

		ResponseEntity<RecordResponse> response = recordController.upsertSingleRecord(instanceId, version, recordType, recordId, Optional.empty(), recordRequest);
		assertThat(response.getStatusCodeValue()).isEqualTo(201);

		// verify the backend datatype
		Map<String, DataTypeMapping> tableSchema = recordDao.getExistingTableSchema(instanceId, recordType);
		assertThat(tableSchema.keySet()).contains(attrName);
		DataTypeMapping actualDataType = tableSchema.get(attrName);
		assertThat(actualDataType).isEqualTo(expectedDataType);

		// get the TSV
		HttpHeaders headers = new HttpHeaders();
		ResponseEntity<Resource> stream = restTemplate.exchange("/{instanceId}/tsv/{version}/{recordType}",
				HttpMethod.GET, new HttpEntity<>(headers), Resource.class, instanceId, version, recordType);

		// read the TSV contents. Note that this test intentionally reads the TSV as strings - we don't use
		// a MappingIterator<RecordAttributes>, for example. This allows us to assert on the string representation
		// of values within the TSV.
		try (
				InputStream inputStream = stream.getBody().getInputStream();
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
		) {
			String firstLine = reader.readLine();
			assertThat(firstLine).isEqualTo("sys_name\t" + attrName);

			String secondLine = reader.readLine();
			assertThat(secondLine).isEqualTo(recordId + "\t" + expectedCellValue);
		}
	}
}
