package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface StreamingWriteHandler extends Closeable {


	/**
	 * Reads numRecords from the stream unless the operation type changes during the
	 * stream in which case we return early and keep the last record read in memory
	 * so it can be returned in a subsequent call.
	 *
	 * @param numRecords max number of records to read
	 * @return info about the records that were read
	 * @throws IOException on error
	 */
	WriteStreamInfo readRecords(int numRecords) throws IOException;


	class WriteStreamInfo {

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
