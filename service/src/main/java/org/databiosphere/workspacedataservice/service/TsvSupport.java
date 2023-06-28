package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.databiosphere.workspacedataservice.shared.model.Record;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public class TsvSupport {

	private TsvSupport() {
	}

	public static CSVFormat getOutputFormat(List<String> headers) {
		return CSVFormat.DEFAULT.builder().setDelimiter('\t').setQuoteMode(QuoteMode.MINIMAL)
				.setHeader(headers.toArray(new String[0])).build();
	}

	public static class RecordEmitter implements Consumer<Record> {

		private final CSVPrinter csvPrinter;
		private final List<String> attributeNames;

		private final ObjectMapper objectMapper;

		public RecordEmitter(CSVPrinter csvPrinter, List<String> attributeNames, ObjectMapper objectMapper) {
			this.csvPrinter = csvPrinter;
			this.attributeNames = attributeNames;
			this.objectMapper = objectMapper;
		}

		@Override
		public void accept(Record rcd) {
			try {
				csvPrinter.print(rcd.getId());
				for (String attributeName : attributeNames) {
					Object attributeValue = rcd.getAttributeValue(attributeName);
					csvPrinter.print(attributeValue != null && attributeValue.getClass().isArray() ? objectMapper.writeValueAsString(attributeValue) : attributeValue);
				}
				csvPrinter.println();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
