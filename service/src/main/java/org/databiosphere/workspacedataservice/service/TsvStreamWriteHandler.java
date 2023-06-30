package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidTsvException;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Stream-reads inbound TSV data using a Jackson CsvMapper;
 *  returns a WriteStreamInfo containing a batch of Records.
 */
public class TsvStreamWriteHandler implements StreamingWriteHandler {

	private final Spliterator<Record> recordSpliterator;

	private final InputStream inputStream;
	private final MappingIterator<RecordAttributes> tsvIterator;

	private final String resolvedPrimaryKey;

	public TsvStreamWriteHandler(InputStream inputStream, ObjectReader tsvReader, RecordType recordType, Optional<String> primaryKey) throws IOException {
		this.inputStream = inputStream;
		try {
			this.tsvIterator = tsvReader.readValues(inputStream);
		} catch (Exception e) {
			throw new InvalidTsvException("Error reading TSV. Please check the format of your upload. " +
					"Underlying error is " + e.getClass().getSimpleName() + ": " + e.getMessage());
		}

		// check for no rows in TSV
		if (!tsvIterator.hasNext()) {
			throw new InvalidTsvException("We could not parse any data rows in your tsv file.");
		}

		// extract column names from the schema, throwing an error if we detect any duplicate column names.
		List<String> colNames;
		FormatSchema formatSchema = tsvIterator.getParser().getSchema();
		if (formatSchema instanceof CsvSchema actualSchema) {
			colNames = StreamSupport.stream(actualSchema.spliterator(), false)
					.map(CsvSchema.Column::getName)
					.toList();

			if (colNames.stream().anyMatch(col -> Collections.frequency(colNames, col) > 1 && !col.isBlank())) {
				throw new InvalidTsvException("TSV contains duplicate column names. "
					+ "Please use distinct column names to prevent overwriting data");
			}
		} else {
			throw new InvalidTsvException("Could not determine primary key column; unexpected schema type:" + formatSchema.getSchemaType());
		}

		// if a primary key is specified, check if it is present in the TSV
		if (primaryKey.isPresent() && !colNames.contains(primaryKey.get())) {
			throw new InvalidTsvException(
					"Uploaded TSV is either missing the " + primaryKey.get()
							+ " column or has a null or empty string value in that column");
		}

		// if primary key is not specified, use the leftmost column
		String resolvedPK = primaryKey.orElseGet( () -> colNames.get(0) );
		resolvedPrimaryKey = resolvedPK;

		// convert the tsvIterator, which is a MappingIterator<RecordAttributes>, to a Stream<Record>
		Stream<RecordAttributes> tsvStream = StreamSupport.stream(
				Spliterators.spliteratorUnknownSize(tsvIterator, Spliterator.ORDERED), false);
		recordSpliterator = rowsToRecords(tsvStream, recordType, resolvedPK).spliterator();
	}

	/**
	 * Reads the next numRecords from the stream's spliterator and returns that batch.
	 * All TSV uploads are UPSERT.
	 * 
	 * @param numRecords size of the batch to read
	 * @return batch of Records
	 */
	@SuppressWarnings("StatementWithEmptyBody")
	public WriteStreamInfo readRecords(int numRecords) {
		try {
			List<Record> result = new ArrayList<>(numRecords);
			for (int i = 0; i < numRecords && recordSpliterator.tryAdvance(result::add); i++) {
				// noop; the action happens in result:add
			}
			return new WriteStreamInfo(result, OperationType.UPSERT);
		} catch (InvalidTsvException ite) {
			// if we already have an InvalidTsvException, just rethrow it
			throw ite;
		} catch (Exception e) {
			// but if we catch something else, wrap it with a more helpful error message
			throw new InvalidTsvException("Error reading TSV. Please check the format of your upload. " +
					"Underlying error is " + e.getClass().getSimpleName() + ": " + e.getMessage());
		}
	}

	@Override
	public void close() throws IOException {
		inputStream.close();
		tsvIterator.close();
	}

	private Record tsvRowToRecord(RecordAttributes row, RecordType recordType, String primaryKey) {
		Object recordId = row.getAttributeValue(primaryKey);
		if (recordId == null || StringUtils.isBlank(recordId.toString())) {
			throw new InvalidTsvException(
					"Uploaded TSV is either missing the " + primaryKey
							+ " column or has a null or empty string value in that column");
		}
		row.removeAttribute(primaryKey);
		row.removeNullHeaders();
		return new Record(recordId.toString(), recordType, row);
	}

	private Stream<Record> rowsToRecords(Stream<RecordAttributes> rows, RecordType recordType, String primaryKey) {
		HashSet<String> recordIds = new HashSet<>(); // this set may be slow for very large TSVs
		return rows.map( m -> {
			Record r = tsvRowToRecord(m, recordType, primaryKey);
			if (!recordIds.add(r.getId())) {
				throw new InvalidTsvException("TSVs cannot contain duplicate primary key values");
			}
			return r;
		});
	}

	public String getResolvedPrimaryKey() {
		return resolvedPrimaryKey;
	}
}
