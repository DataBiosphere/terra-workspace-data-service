package org.databiosphere.workspacedataservice.metrics;

public class MetricsDefinitions {

  /** counter for column schema changes, i.e. "alter column" sql statements */
  public static final String COUNTER_COLCHANGE = "column_change_datatype";

  /** tag for a {@link org.databiosphere.workspacedataservice.shared.model.RecordType} */
  public static final String TAG_RECORDTYPE = "RecordType";

  /** tag for an instance id */
  public static final String TAG_INSTANCE = "Instance";

  /** tag for a record attribute name */
  public static final String TAG_ATTRIBUTENAME = "AttributeName";
}
