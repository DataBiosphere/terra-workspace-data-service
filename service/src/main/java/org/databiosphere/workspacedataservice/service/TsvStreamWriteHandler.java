package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidTsvException;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.databiosphere.workspacedataservice.tsv.TsvConverter;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Stream-reads inbound TSV data using a Jackson CsvMapper and WDS's TsvConverter;
 *  returns a WriteStreamInfo containing a batch of Records.
 */
public class TsvStreamWriteHandler implements StreamingWriteHandler {

	private Spliterator<Record> recordSpliterator;

	public TsvStreamWriteHandler(InputStream inputStream, ObjectReader tsvReader, RecordType recordType, Optional<String> primaryKey) throws IOException {
		MappingIterator<RecordAttributes> tsvIterator = tsvReader.readValues(inputStream);

		// check for no rows in TSV
		if (!tsvIterator.hasNext()) {
			throw new InvalidTsvException("We could not parse any data rows in your tsv file.");
		}

		// extract column names from the schema
		List<String> colNames;
		FormatSchema formatSchema = tsvIterator.getParser().getSchema();
		if (formatSchema instanceof CsvSchema actualSchema) {
			colNames = actualSchema.getColumnNames();
		} else {
			throw new InvalidTsvException("Could not determine primary key column; unexpected schema type:" + formatSchema.getSchemaType());
		}

		// if a primary key is specified, check if it is present in the TSV
		if (primaryKey.isPresent() && !colNames.contains(primaryKey.get())) {
			throw new InvalidTsvException(
					"Uploaded TSV is either missing the " + primaryKey
							+ " column or has a null or empty string value in that column");
		}

		// if primary key is not specified, use the leftmost column
		String resolvedPK = primaryKey.orElseGet( () -> colNames.get(0) );

		// convert the tsvIterator, which is a MappingIterator<RecordAttributes>, to a Stream<Record>
		TsvConverter tsvConverter = new TsvConverter();
		Stream<RecordAttributes> tsvStream = StreamSupport.stream(
				Spliterators.spliteratorUnknownSize(tsvIterator, Spliterator.ORDERED), false);
		recordSpliterator = tsvConverter.rowsToRecords(tsvStream, recordType, resolvedPK).spliterator();
	}

	/**
	 * Reads the next numRecords from the stream's spliterator and returns that batch.
	 * All TSV uploads are UPSERT.
	 * 
	 * @param numRecords
	 * @return
	 * @throws IOException
	 */
	public WriteStreamInfo readRecords(int numRecords) throws IOException {
		List<Record> result = new ArrayList<>(numRecords);
		for (int i = 0; i < numRecords && recordSpliterator.tryAdvance(result::add); i++) {
			// noop; the action happens in result:add
		}
		return new WriteStreamInfo(result, OperationType.UPSERT);
	}

	@Override
	public void close() throws IOException {
		// noop, nothing to close
	}


}
