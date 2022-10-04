package org.databiosphere.workspacedataservice.service;

import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;
import org.databiosphere.workspacedataservice.shared.model.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class TsvSupport {


    public static TsvWriterSettings getTsvSettings(){
        TsvWriterSettings settings = new TsvWriterSettings();
        settings.getFormat().setLineSeparator("\n");
        return settings;
    }

    public static class RecordEmitter implements Consumer<Record> {

        private final TsvWriter tsvWriter;
        private final List<String> attributeNames;

        public RecordEmitter(TsvWriter writer, List<String> attributeNames) {
            this.tsvWriter = writer;
            this.attributeNames = attributeNames;
        }

        @Override
        public void accept(Record record) {
            List<Object> attributeValues = new ArrayList<>();
            attributeValues.add(record.getId());
            for (String attributeName : attributeNames) {
                attributeValues.add(record.getAttributeValue(attributeName));
            }
            tsvWriter.writeRow(attributeValues);
        }
    }
}
