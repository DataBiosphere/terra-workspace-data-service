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

public interface StreamingWriteHandler extends Closeable {


	/**
	 * Reads numRecords from the stream unless the operation type changes during the
	 * stream in which case we return early and keep the last record read in memory
	 * so it can be returned in a subsequent call.
	 * 
	 * @param numRecords
	 * @return
	 * @throws IOException
	 */
	public WriteStreamInfo readRecords(int numRecords) throws IOException;


	public static class WriteStreamInfo {

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
