package org.databiosphere.workspacedataservice.tsv;

import org.apache.commons.lang3.StringUtils;
import org.databiosphere.workspacedataservice.service.model.exception.InvalidTsvException;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.stream.Stream;

@Service
public class TsvConverter {


    public Record tsvRowToRecord(RecordAttributes row, RecordType recordType, String primaryKey) {
        Object recordId = row.getAttributeValue(primaryKey);
        if (recordId == null || StringUtils.isBlank(recordId.toString())) {
            throw new InvalidTsvException(
                    "Uploaded TSV is either missing the " + primaryKey
                            + " column or has a null or empty string value in that column");
        }
        row.remove(primaryKey);
        return new Record(recordId.toString(), recordType, row);
    }

    public Stream<Record> rowsToRecords(Stream<RecordAttributes> rows, RecordType recordType, String primaryKey) {
        HashSet<String> recordIds = new HashSet<>(); // this set may be slow for very large TSVs
        return rows.map( m -> {
            Record r = tsvRowToRecord(m, recordType, primaryKey);
            if (!recordIds.add(r.getId())) {
                throw new InvalidTsvException("TSVs cannot contain duplicate primary key values");
            }
            return r;
        });
    }

}
