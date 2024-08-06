package org.databiosphere.workspacedataservice.dataimport.tdr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.databiosphere.workspacedataservice.common.TestBase;
import org.databiosphere.workspacedataservice.recordsource.RecordSource;
import org.databiosphere.workspacedataservice.service.model.TdrManifestImportTable;
import org.databiosphere.workspacedataservice.shared.model.Record;
import org.databiosphere.workspacedataservice.shared.model.RecordAttributes;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;

@SpringBootTest
class ParquetDataTypesTest extends TestBase {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  @Autowired ObjectMapper mapper;

  // a BigQuery export; the BigQuery table contains NUMERIC and BIGNUMERIC columns
  @Value("classpath:parquet/numerics.parquet")
  Resource numericsParquet;

  // a BigQuery export; the BigQuery table contains a NaN in its float column
  @Value("classpath:parquet/nans.parquet")
  Resource nansParquet;

  // contains a sample column which is an array of ints
  @Value("classpath:parquet/with-entity-reference-lists/person.parquet")
  Resource arraysParquet;

  @Value("classpath:parquet/dates-times-azure.parquet")
  Resource datesTimesAzureParquet;

  @Value("classpath:parquet/dates-times-gcp.parquet")
  Resource datesTimesGcpParquet;

  @Test
  void datesAndTimesAzure() throws IOException {
    // ARRANGE - create all the objects we'll need to do the conversions below
    TdrManifestImportTable testTable = makeTable("test", "pk");
    ParquetRecordConverter converter = new ParquetRecordConverter(testTable, mapper);

    InputFile inputFile =
        HadoopInputFile.fromPath(
            new Path(datesTimesAzureParquet.getURL().toString()), new Configuration());

    // ACT - read via ParquetReader and convert via ParquetRecordConverter
    try (ParquetReader<GenericRecord> avroParquetReader =
        TdrManifestQuartzJob.readerForFile(inputFile)) {

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

      assertThat(records.size()).isEqualTo(1);

      // ASSERT
      Record record = records.get(0);

      // Parquet files exported by Azure TDR do not include logical types for times, datetimes, or
      // timestamps.
      // Nor do they properly format arrays (AJ-1735)
      // Thus, date is the only type we can test here.
      assertThat(record.getAttributeValue("date")).isEqualTo(LocalDate.of(2024, 6, 3));
    }
  }

  @Test
  void datesAndTimesGcp() throws IOException {
    // ARRANGE - create all the objects we'll need to do the conversions below
    TdrManifestImportTable testTable = makeTable("test", "pk");
    ParquetRecordConverter converter = new ParquetRecordConverter(testTable, mapper);

    InputFile inputFile =
        HadoopInputFile.fromPath(
            new Path(datesTimesGcpParquet.getURL().toString()), new Configuration());

    // ACT - read via ParquetReader and convert via ParquetRecordConverter
    try (ParquetReader<GenericRecord> avroParquetReader =
        TdrManifestQuartzJob.readerForFile(inputFile)) {

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

      assertThat(records.size()).isEqualTo(1);

      // ASSERT
      Record record = records.get(0);

      assertThat(record.getAttributeValue("date")).isEqualTo(LocalDate.of(2024, 6, 3));
      assertThat(record.getAttributeValue("date_array"))
          .isEqualTo(List.of(LocalDate.of(2024, 6, 3)));

      assertThat(record.getAttributeValue("datetime"))
          .isEqualTo(LocalDateTime.of(2024, 6, 3, 10, 30, 0));
      assertThat(record.getAttributeValue("datetime_array"))
          .isEqualTo(List.of(LocalDateTime.of(2024, 6, 3, 10, 30, 0)));

      assertThat(record.getAttributeValue("timestamp"))
          .isEqualTo(LocalDateTime.of(2024, 6, 3, 10, 30, 0));
      assertThat(record.getAttributeValue("timestamp_array"))
          .isEqualTo(List.of(LocalDateTime.of(2024, 6, 3, 10, 30, 0)));

      assertThat(record.getAttributeValue("time")).isEqualTo("10:30:00");
      assertThat(record.getAttributeValue("time_array")).isEqualTo(List.of("10:30:00"));
    }
  }

  /**
   * Given an input Parquet file, convert the Parquet to WDS Records/attributes and assert on the
   * converted datatypes/values
   *
   * @throws IOException on IO problem
   */
  @Test
  void numericPrecision() throws IOException {
    // ARRANGE - create all the objects we'll need to do the conversions below
    TdrManifestImportTable testTable = makeTable("test", "pk");
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
      assertThat(records).hasSize(3);

      // Each column in numeric.parquet has the same values, but
      // each column has a different schema. The [0, 0.01, 0.22] values are the expected
      // values in numerics.parquet.
      List.of("numeric_default", "numeric_custom", "bignumeric_default", "bignumeric_custom")
          .forEach(
              attributeName ->
                  assertThat(getColumnValues(records, attributeName))
                      .describedAs("for column %s", attributeName)
                      .containsExactlyInAnyOrder(0, 0.01, 0.22));
    }
  }

  @Test
  void nans() throws IOException {
    // ARRANGE - create all the objects we'll need to do the conversions below
    TdrManifestImportTable testTable = makeTable("test", "int");
    ParquetRecordConverter converter = new ParquetRecordConverter(testTable, mapper);

    InputFile numericsFile =
        HadoopInputFile.fromPath(new Path(nansParquet.getURL().toString()), new Configuration());

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
      assertThat(records).hasSize(4);

      // the records that have an int of 1, 2, and 4 should also have a float of 1, 2, 4.
      // the record that has an int of 3 should have a null float. We treat NaN as null.

      records.forEach(
          rec -> {
            Number intAttr = (Number) rec.getAttributeValue("int");

            var floatAttr = rec.getAttributeValue("float");

            if (intAttr.intValue() == 3) {
              assertNull(floatAttr);
            } else {
              assertEquals(intAttr.floatValue(), ((Number) floatAttr).floatValue());
            }
          });
    }
  }

  @Test
  void arrays() throws IOException {
    // ARRANGE - create all the objects we'll need to do the conversions below
    TdrManifestImportTable testTable = makeTable("person", "id");
    ParquetRecordConverter converter = new ParquetRecordConverter(testTable, mapper);

    InputFile arraysFile =
        HadoopInputFile.fromPath(new Path(arraysParquet.getURL().toString()), new Configuration());

    // ACT - read via ParquetReader and convert via ParquetRecordConverter
    try (ParquetReader<GenericRecord> avroParquetReader =
        TdrManifestQuartzJob.readerForFile(arraysFile)) {

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
      assertThat(records).hasSize(3);

      // map of id -> expected samples value
      var expectedMap =
          Map.of(
              1,
              List.of(BigDecimal.valueOf(1), BigDecimal.valueOf(2)),
              2,
              List.of(BigDecimal.valueOf(3)),
              3,
              List.of(BigDecimal.valueOf(4), BigDecimal.valueOf(5)));

      expectedMap.forEach(
          (id, expected) -> {
            // find record with this id
            List<Record> found =
                records.stream().filter(rec -> id.toString().equals(rec.getId())).toList();
            assertThat(found).hasSize(1);
            Record actualRecord = found.get(0);
            // find samples value
            Object actual = actualRecord.getAttributeValue("samples");
            // assert
            List<Object> actualList = assertInstanceOf(List.class, actual);
            assertThat(actualList).containsExactlyElementsOf(expected);
          });
    }
  }

  // This is set up to replicate the scenario detected by AJ-1844
  @Test
  void primaryKeyIsNormallyIncluded() {
    // arrange
    TdrManifestImportTable testTable = makeTable("ArraysInputsTable", "chip_well_barcode");
    ParquetRecordConverter converter = new ParquetRecordConverter(testTable, mapper);

    GenericData.Record genericRecord =
        new GenericRecordBuilder(
                schema(
                    "ArraysInputsTable",
                    field("chip_well_barcode", Type.STRING),
                    field("lab_batch", Type.STRING)))
            .set("chip_well_barcode", "123")
            .set("lab_batch", "456")
            .build();

    // act
    Record result = converter.convertBaseAttributes(genericRecord);

    // assert
    assertThat(result.getId()).isEqualTo("123");
    assertThat(result.getAttributes())
        .isEqualTo(
            new RecordAttributes(
                new ImmutableMap.Builder<String, Object>()
                    .put("chip_well_barcode", "123")
                    .put("lab_batch", "456")
                    .build()));
  }

  // even though the source TDR manifest contains a column of ${entityType}_id, translation should
  // include that column. The RawlsAttributePrefixer will later rename this to tdr:${entityType}_id.
  @Test
  void primaryKeyIsNotSkippedWhenMatchingTypeId() {
    // arrange
    TdrManifestImportTable testTable = makeTable("someTable", "someTable_id");
    ParquetRecordConverter converter = new ParquetRecordConverter(testTable, mapper);

    GenericData.Record genericRecord =
        new GenericRecordBuilder(
                schema(
                    "ArraysInputsTable",
                    field("someTable_id", Type.STRING),
                    field("someField", Type.STRING)))
            .set("someTable_id", "123")
            .set("someField", "456")
            .build();

    // act
    Record result = converter.convertBaseAttributes(genericRecord);

    // assert
    assertThat(result.getId()).isEqualTo("123");
    assertThat(result.getAttributes())
        .isEqualTo(new RecordAttributes(Map.of("someField", "456", "someTable_id", "123")));
  }

  private TdrManifestImportTable makeTable(String recordTypeName, String primaryKey) {
    return new TdrManifestImportTable(
        RecordType.valueOf(recordTypeName),
        primaryKey,
        /* dataFiles= */ List.of(),
        /* relations= */ List.of());
  }

  private Schema schema(String tableName, Field... fields) {
    return Schema.createRecord(
        tableName, "doc", "namespace", /* isError= */ false, List.of(fields));
  }

  private Field field(String name, Type type) {
    return new Field(name, Schema.create(type));
  }

  private List<Object> getColumnValues(List<Record> records, String attributeName) {
    return records.stream().map(rec -> rec.getAttributeValue(attributeName)).toList();
  }
}
