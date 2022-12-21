package org.databiosphere.workspacedataservice.service;

import org.databiosphere.workspacedataservice.shared.model.OperationType;
import org.databiosphere.workspacedataservice.shared.model.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Stream;

public class TsvStreamWriteHandler implements StreamingWriteHandler {

	private final Spliterator<Record> spliterator;

	public TsvStreamWriteHandler(Stream<Record> upsertRecords) throws IOException {
		this.spliterator = upsertRecords.spliterator();
	}

	/**
	 * Reads numRecords from the stream unless the operation type changes during the
	 * stream in which case we return early and keep the last record read in memory,
	 * so it can be returned in a subsequent call.
	 * 
	 * @param numRecords
	 * @return
	 * @throws IOException
	 */
	public WriteStreamInfo readRecords(int numRecords) throws IOException {
		List<Record> result = new ArrayList<>(numRecords);
		for (int i = 0; i < numRecords && spliterator.tryAdvance(result::add); i++) {
			// noop; the action happens in result:add
		}
		return new WriteStreamInfo(result, OperationType.UPSERT);
	}

	@Override
	public void close() throws IOException {
		// noop, nothing to close
	}


}
