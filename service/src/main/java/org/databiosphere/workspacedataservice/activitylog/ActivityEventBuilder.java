package org.databiosphere.workspacedataservice.activitylog;

import org.databiosphere.workspacedataservice.sam.SamDao;
import org.springframework.web.context.request.RequestContextHolder;

import java.util.Objects;
import java.util.UUID;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

public class ActivityEventBuilder {

    private final ActivityLogger activityLogger;
    private final SamDao samDao;

    private String subject;
    private ActivityModels.Action action;
    private ActivityModels.Thing thing;
    private String[] ids;


    // ===== SUBJECT BUILDERS

    public ActivityEventBuilder currentUser() {
        try {
            // grab the current user's bearer token (see BearerTokenFilter)
            Object token = RequestContextHolder.currentRequestAttributes()
                    .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);
            // resolve the token to a user id via Sam
            this.subject = samDao.getUserId(token.toString());
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

//    public ActivityEventBuilder setAction(ActivityModels.Action action) {
//        this.action = action;
//        return this;
//    }

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
//
//    public ActivityEventBuilder setThing(ActivityModels.Thing thing) {
//        this.thing = thing;
//        return this;
//    }

    public ActivityEventBuilder(ActivityLogger activityLogger, SamDao samDao) {
        this.activityLogger = activityLogger;
        this.samDao = samDao;
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

    public ActivityEventBuilder withIds(String[] ids) {
        this.ids = ids;
        return this;
    }


    public void persist() {
        this.activityLogger.saveActivity(build());
    }

    public ActivityEvent build() {
        return new ActivityEvent(subject, action, (long) ids.length, thing, ids);
    }
}