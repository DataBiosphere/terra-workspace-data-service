package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.shared.model.BatchOperation;
import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class StreamingWriteHandler implements Closeable {

	private final JsonParser parser;

	private BatchOperation savedRecord;

	private final InputStream inputStream;

	public StreamingWriteHandler(InputStream inputStream, ObjectMapper objectMapper) throws IOException {
		this.inputStream = inputStream;
		JsonFactory factory = new JsonFactory(objectMapper.copy());
		parser = factory.createParser(inputStream);
		if (parser.nextToken() != JsonToken.START_ARRAY) {
			throw new IllegalArgumentException("Expected content to be an array");
		}
	}

	/**
	 * Reads numRecords from the stream unless the operation type changes during the
	 * stream in which case we return early and keep the last record read in memory
	 * so it can be returned in a subsequent call.
	 * 
	 * @param numRecords
	 * @return
	 * @throws IOException
	 */
	public WriteStreamInfo readRecords(int numRecords) throws IOException {
		int recordsProcessed = 0;
		List<Record> result = new ArrayList<>(numRecords);
		OperationType lastOp = null;
		if (savedRecord != null) {
			result.add(savedRecord.getRecord());
			lastOp = savedRecord.getOperation();
			savedRecord = null;
		}
		// order matters in this condition, we don't want to advance the parser (call
		// nextToken()) unless we're
		// ready to consume the next BatchOperation
		while (recordsProcessed < numRecords && parser.nextToken() != JsonToken.END_ARRAY && parser.hasCurrentToken()) {
			BatchOperation op = parser.readValueAs(BatchOperation.class);
			OperationType opType = op.getOperation();
			if (lastOp != null && lastOp != opType) {
				savedRecord = op;
				return new WriteStreamInfo(result, lastOp);
			}
			recordsProcessed++;
			result.add(op.getRecord());
			lastOp = opType;
		}
		return new WriteStreamInfo(result, lastOp);
	}

	@Override
	public void close() throws IOException {
		parser.close();
		inputStream.close();
	}

	public class WriteStreamInfo {

		private final List<Record> records;

		private final OperationType operationType;

		public WriteStreamInfo(List<Record> records, OperationType operationType) {
			this.records = records;
			this.operationType = operationType;
		}

		public List<Record> getRecords() {
			return records;
		}

		public OperationType getOperationType() {
			return operationType;
		}
	}

}
