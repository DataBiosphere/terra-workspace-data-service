package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.databiosphere.workspacedataservice.shared.model.Record;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Stream;


public class TsvSupport {

	private TsvSupport() {
	}

	public static void WriteCsvToStream (Stream<Record> records, OutputStream stream, List<String> headers) throws IOException {

		CsvSchema tsvHeaderSchema = CsvSchema.emptySchema()
		.withEscapeChar('\\')
		.withColumnSeparator('\t');

		final CsvMapper tsvMapper = CsvMapper.builder()
		.build();

		SequenceWriter seqW = tsvMapper.writer(tsvHeaderSchema)
			.writeValues(stream);
		seqW.write(headers);
		for (Record record : records.toList()) {
			List<Object> row = RecordToRow(record, headers);
			seqW.write(row);
		}
		seqW.close();		
	}

	private static List<Object> RecordToRow(Record record, List<String> headers) {
		List<Object> row = new ArrayList<Object>();
		row.add(record.getId());
		headers.forEach(h -> {
			row.add(record.getAttributeValue(h));
		});
		return row;
	}
}
