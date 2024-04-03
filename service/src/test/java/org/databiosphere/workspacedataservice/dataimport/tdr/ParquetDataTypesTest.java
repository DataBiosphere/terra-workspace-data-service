package org.databiosphere.workspacedataservice.dataimport.tdr;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.recordsource.RecordSource;
import org.databiosphere.workspacedataservice.service.model.TdrManifestImportTable;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;

@SpringBootTest
class ParquetDataTypesTest extends TestBase {

  @Autowired ObjectMapper mapper;

  // a BigQuery export; the BigQuery table contains NUMERIC and BIGNUMERIC columns
  @Value("classpath:parquet/numerics.parquet")
  Resource numericsParquet;

  /**
   * Given an input Parquet file, convert the Parquet to WDS Records/attributes and assert on the
   * converted datatypes/values
   *
   * @throws IOException on IO problem
   */
  @Test
  void numericPrecision() throws IOException {
    // ARRANGE - create all the objects we'll need to do the conversions below
    TdrManifestImportTable testTable =
        new TdrManifestImportTable(RecordType.valueOf("test"), "pk", List.of(), List.of());

    ParquetRecordConverter converter = new ParquetRecordConverter(testTable, mapper);

    InputFile numericsFile =
        HadoopInputFile.fromPath(
            new Path(numericsParquet.getURL().toString()), new Configuration());

    // ACT - read via ParquetReader and convert via ParquetRecordConverter
    try (ParquetReader<GenericRecord> avroParquetReader =
        TdrManifestQuartzJob.readerForFile(numericsFile)) {

      // read the Parquet file into List<GenericRecord> via the parquet-hadoop library
      List<GenericRecord> genericRecords = new ArrayList<>();
      while (true) {
        GenericRecord rec = avroParquetReader.read();
        if (rec == null) {
          break;
        }
        genericRecords.add(rec);
      }

      // convert the GenericRecords into WDS Records. For this test we only care about
      // BASE_ATTRIBUTES
      List<Record> records =
          genericRecords.stream()
              .map(
                  genericRecord ->
                      converter.convert(genericRecord, RecordSource.ImportMode.BASE_ATTRIBUTES))
              .toList();

      // ASSERT
      // the expected number of records
      assertThat(records.size()).isEqualTo(3);

      // Each column in numeric.parquet has the same values, but
      // each column has a different schema. The [0, 0.01, 0.22] values are the expected
      // values in numerics.parquet.
      List.of("numeric_default", "numeric_custom", "bignumeric_default", "bignumeric_custom")
          .forEach(
              attributeName ->
                  assertThat(getColumnValues(records, attributeName))
                      .withFailMessage("for attribute %s", attributeName)
                      .containsExactlyInAnyOrder(0, 0.01, 0.22));
    }
  }

  private List<Object> getColumnValues(List<Record> records, String attributeName) {
    return records.stream().map(rec -> rec.getAttributeValue(attributeName)).toList();
  }
}
