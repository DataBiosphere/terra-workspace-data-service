package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.service.model.exception.UnexpectedTsvException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Stream;

import static org.databiosphere.workspacedataservice.service.model.DataTypeMapping.JSON;

@Component
public class TsvSupport {

	private final ObjectMapper objectMapper;

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private TsvSupport(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public void writeTsvToStream (Stream<Record> records, Map<String, DataTypeMapping> typeSchema, OutputStream stream, List<String> headers) throws IOException {

		CsvSchema tsvHeaderSchema = CsvSchema.emptySchema()
				.withEscapeChar('\\')
				.withColumnSeparator('\t');

		final CsvMapper tsvMapper = CsvMapper.builder()
				.enable(CsvGenerator.Feature.STRICT_CHECK_FOR_QUOTING)
				.build();

		SequenceWriter seqW = tsvMapper.writer(tsvHeaderSchema)
				.writeValues(stream);
		seqW.write(headers);
		// First header is Primary Key, and value is stored in rcd.id. Remove header here and add rcd.id manually.
		headers.remove(0);
		records.forEach(rcd -> writeRowToTsv(seqW, rcd, typeSchema, headers));
		seqW.close();
	}

	private void writeRowToTsv(SequenceWriter seqW, Record rcd, Map<String, DataTypeMapping> typeSchema, List<String> headers) {
		try {
			List<String> row = recordToRow(rcd, typeSchema, headers);
			seqW.write(row);
		} catch (Exception e) {
			throw new UnexpectedTsvException("Error writing TSV: " + e.getMessage());
		}
	}

	private List<String> recordToRow(Record rcd, Map<String, DataTypeMapping> typeSchema, List<String> headers) {
		List<String> row = new ArrayList<>();
		row.add(rcd.getId());
		headers.forEach(h -> {
			Object attr = rcd.getAttributeValue(h);
			DataTypeMapping dataType = typeSchema.get(h);

			if (dataType.isArrayType() || JSON.equals(dataType)) {
				try {
					row.add(objectMapper.writeValueAsString(attr));
				} catch (JsonProcessingException e) {
					row.add(attr.toString());
					logger.warn("Failed to properly serialize value of type {} to TSV: {}", dataType.name(), e.getMessage());
				}
			} else {
				row.add(attr == null ? "" : attr.toString());
			}

		});
		return row;
	}


}
