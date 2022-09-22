package org.databiosphere.workspacedataservice.service;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.databiosphere.workspacedataservice.service.model.DataTypeMapping;
import org.databiosphere.workspacedataservice.shared.model.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StreamingWriteHandler {

    private final JsonParser parser;

    public StreamingWriteHandler(InputStream inputStream) throws IOException {
        JsonFactory factory = new JsonFactory(new ObjectMapper());
        parser = factory.createParser(inputStream);
        if (parser.nextToken() != JsonToken.START_ARRAY) {
            throw new IllegalArgumentException("Expected content to be an array");
        }
    }

    public List<Record> readRecords(int numRecords) throws IOException {
        int recordsProcessed = 0;
        List<Record> result = new ArrayList<>(numRecords);
        while(parser.nextToken() != JsonToken.END_ARRAY && recordsProcessed < numRecords){
            Record record = parser.readValueAs(Record.class);
            recordsProcessed++;
            result.add(record);
        }
        return result;
    }

    public class RecordsWithSchema {

        private final Map<String, DataTypeMapping> schema;

        private final List<Record> records;

        public RecordsWithSchema(Map<String, DataTypeMapping> schema, List<Record> records) {
            this.schema = schema;
            this.records = records;
        }

        public Map<String, DataTypeMapping> getSchema() {
            return schema;
        }

        public List<Record> getRecords() {
            return records;
        }
    }


}
