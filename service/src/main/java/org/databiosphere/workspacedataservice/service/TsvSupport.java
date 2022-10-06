package org.databiosphere.workspacedataservice.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.databiosphere.workspacedataservice.service.model.ReservedNames;
import org.databiosphere.workspacedataservice.shared.model.Record;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

public class TsvSupport {

	private TsvSupport() {
	}
    public static final String ROW_ID_COLUMN_NAME = ReservedNames.RECORD_ID;


    public static CSVFormat getUploadFormat(){
        return CSVFormat.Builder.create(CSVFormat.DEFAULT).setHeader()
                .setDelimiter('\t')
                .setQuoteMode(QuoteMode.MINIMAL).build();
    }

    public static CSVFormat getOutputFormat(List<String> headers) {
         return CSVFormat.DEFAULT.builder().setDelimiter('\t')
                .setQuoteMode(QuoteMode.MINIMAL)
                 .setRecordSeparator("\n")
                 .setHeader(headers.toArray(new String[0])).build();
    }

	public static class RecordEmitter implements Consumer<Record> {

		private final CSVPrinter csvPrinter;
		private final List<String> attributeNames;

		public RecordEmitter(CSVPrinter csvPrinter, List<String> attributeNames) {
			this.csvPrinter = csvPrinter;
			this.attributeNames = attributeNames;
		}

		@Override
		public void accept(Record rcd) {
			try {
				csvPrinter.print(rcd.getId());
				for (String attributeName : attributeNames) {
					csvPrinter.print(rcd.getAttributeValue(attributeName));
				}
				csvPrinter.println();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
