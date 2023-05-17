package org.databiosphere.workspacedataservice.activitylog;

import org.databiosphere.workspacedataservice.sam.SamDao;
import org.databiosphere.workspacedataservice.shared.model.RecordType;
import org.springframework.web.context.request.RequestContextHolder;

import java.util.UUID;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

public class ActivityEventBuilder {

    private final ActivityLogger activityLogger;
    private final SamDao samDao;

    private String subject;
    private ActivityModels.Action action;
    private ActivityModels.Thing thing;
    private RecordType recordType;
    private int quantity;
    private String[] ids;

    /**
     * Takes a reference to the ActivityLogger that created this event, and which
     * will eventually persist the event (to a log or to a db table)
     * @param activityLogger the parent ActivityLogger
     * @param samDao Sam dao to use for resolving the current user to a Sam id
     */
    public ActivityEventBuilder(ActivityLogger activityLogger, SamDao samDao) {
        this.activityLogger = activityLogger;
        this.samDao = samDao;
    }


    // ===== SUBJECT BUILDERS

    public ActivityEventBuilder currentUser() {
        try {
            // grab the current user's bearer token (see BearerTokenFilter)
            Object token = RequestContextHolder.currentRequestAttributes()
                    .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);
            if (token != null) {
                // resolve the token to a user id via Sam
                this.subject = samDao.getUserId(token.toString());
            } else {
                this.subject = "anonymous";
            }

        } catch (Exception e) {
            this.subject = "???";
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
    public ActivityEventBuilder modified() {
        this.action = ActivityModels.Action.MODIFY;
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

    // RECORD TYPE BUILDERS
    public ActivityEventBuilder withRecordType(RecordType recordType) {
        this.recordType = recordType;
        return this;
    }

    // QUANTITY BUILDERS
    public ActivityEventBuilder ofQuantity(int quantity) {
        this.quantity = quantity;
        return this;
    }

    // ID BUILDERS

    public ActivityEventBuilder withId(String id) {
        this.ids = new String[]{id};
        return this;
    }

    public ActivityEventBuilder withUuid(UUID id) {
        this.ids = new String[]{id.toString()};
        return this;
    }

    public void persist() {
        this.activityLogger.saveEvent(build());
    }

    private ActivityEvent build() {
        return new ActivityEvent(subject, action, thing, recordType, quantity, ids);
    }
}