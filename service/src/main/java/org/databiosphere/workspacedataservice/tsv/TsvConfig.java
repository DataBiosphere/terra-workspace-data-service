package org.databiosphere.workspacedataservice.tsv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.databiosphere.workspacedataservice.service.DataTypeInferer;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TsvConfig {
    @Bean
    public TsvDeserializer tsvDeserializer(DataTypeInferer inferer, ObjectMapper objectMapper) {
        return new TsvDeserializer(inferer, objectMapper);
    }

    @Bean
    public ObjectReader tsvReader(TsvDeserializer tsvDeserializer) {
        // read schema (column names) from the input file's header
        CsvSchema tsvHeaderSchema = CsvSchema.emptySchema()
                .withHeader()
                .withEscapeChar('\\')
                .withColumnSeparator('\t');

        final CsvMapper tsvMapper = CsvMapper.builder()
                .enable(CsvParser.Feature.SKIP_EMPTY_LINES)
                .enable(CsvParser.Feature.EMPTY_STRING_AS_NULL)
                .build();

        SimpleModule module = new SimpleModule();
        module.addDeserializer(RecordAttributes.class, tsvDeserializer);
        tsvMapper.registerModule(module);

        return tsvMapper
                .readerFor(RecordAttributes.class)
                .with(tsvHeaderSchema);
    }

}
