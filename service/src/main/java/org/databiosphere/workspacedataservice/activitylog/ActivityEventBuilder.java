package org.databiosphere.workspacedataservice.activitylog;

import java.util.UUID;
import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.sam.TokenContextUtil;
import org.databiosphere.workspacedataservice.shared.model.BearerToken;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builder for ActivityEvent, with many convenience functions */
public class ActivityEventBuilder {

  private final SamDao samDao;

  private String subject;
  private ActivityModels.Action action;
  private ActivityModels.Thing thing;
  private RecordType recordType;
  private int quantity;
  private String[] ids;

  private static final Logger LOGGER = LoggerFactory.getLogger(ActivityEventBuilder.class);

  /**
   * Constructor.
   *
   * @param samDao Sam dao to use for resolving the current user to a Sam id
   */
  public ActivityEventBuilder(SamDao samDao) {
    this.samDao = samDao;
  }

  // ===== SUBJECT BUILDERS

  /** initializes this builder with the current user's Sam id. */
  public ActivityEventBuilder currentUser() {
    try {
      // grab the current user's bearer token (see BearerTokenFilter)
      BearerToken token = TokenContextUtil.getToken();
      if (token.nonEmpty()) {
        // resolve the token to a user id via Sam
        this.subject = samDao.getUserId(token.getValue());
      } else {
        this.subject = "anonymous";
      }

    } catch (Exception e) {
      LOGGER.warn("Error resolving user token to id via Sam: " + e.getMessage(), e);
      this.subject = "(unknown due to exception)";
    }
    return this;
  }

  // ===== VERB BUILDERS

  public ActivityEventBuilder created() {
    this.action = ActivityModels.Action.CREATE;
    return this;
  }

  public ActivityEventBuilder deleted() {
    this.action = ActivityModels.Action.DELETE;
    return this;
  }

  public ActivityEventBuilder updated() {
    this.action = ActivityModels.Action.UPDATE;
    return this;
  }

  public ActivityEventBuilder upserted() {
    this.action = ActivityModels.Action.UPSERT;
    return this;
  }

  public ActivityEventBuilder started() {
    this.action = ActivityModels.Action.STARTED;
    return this;
  }

  public ActivityEventBuilder completed() {
    this.action = ActivityModels.Action.COMPLETED;
    return this;
  }

  public ActivityEventBuilder modified() {
    this.action = ActivityModels.Action.MODIFY;
    return this;
  }

  public ActivityEventBuilder linked() {
    this.action = ActivityModels.Action.LINK;
    return this;
  }

  public ActivityEventBuilder restored() {
    this.action = ActivityModels.Action.RESTORE;
    return this;
  }

  // OBJECT BUILDERS

  public ActivityEventBuilder instance() {
    this.thing = ActivityModels.Thing.INSTANCE;
    return this;
  }

  public ActivityEventBuilder table() {
    this.thing = ActivityModels.Thing.TABLE;
    return this;
  }

  public ActivityEventBuilder record() {
    this.thing = ActivityModels.Thing.RECORD;
    return this;
  }

  public ActivityEventBuilder snapshotReference() {
    this.thing = ActivityModels.Thing.SNAPSHOT_REFERENCE;
    return this;
  }

  public ActivityEventBuilder importTables() {
    this.thing = ActivityModels.Thing.IMPORT_TABLES;
    return this;
  }

  public ActivityEventBuilder backup() {
    this.thing = ActivityModels.Thing.BACKUP;
    return this;
  }

  // RECORD TYPE BUILDERS
  public ActivityEventBuilder withRecordType(RecordType recordType) {
    this.recordType = recordType;
    return this;
  }

  // QUANTITY BUILDERS
  /**
   * specifies the quantity of items being operated on. If ids are specified instead, the
   * id[].length will take precedence over this quantity.
   */
  public ActivityEventBuilder ofQuantity(int quantity) {
    this.quantity = quantity;
    return this;
  }

  // ID BUILDERS

  public ActivityEventBuilder withId(String id) {
    this.ids = new String[] {id};
    return this;
  }

  public ActivityEventBuilder withUuid(UUID id) {
    this.ids = new String[] {id.toString()};
    return this;
  }

  protected ActivityEvent build() {
    return new ActivityEvent(subject, action, thing, recordType, quantity, ids);
  }
}
