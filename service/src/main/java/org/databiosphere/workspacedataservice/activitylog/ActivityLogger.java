package org.databiosphere.workspacedataservice.activitylog;

import org.databiosphere.workspacedataservice.sam.SamDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Save entries to an activity log. Currently implemented as writing to
 * a Slf4j logger, but could be modified to write entries to a database
 * table or other persistence.
 */
public class ActivityLogger {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActivityLogger.class);

    private final SamDao samDao;

    public ActivityLogger(SamDao samDao) {
        this.samDao = samDao;
    }


    /**
     * creates a new, empty event. In most cases, callers should use
     * #saveEventForCurrentUser instead.
     * @return event builder.
     */
    public ActivityEventBuilder newEvent() {
        return new ActivityEventBuilder(this.samDao);
    }

    /**
     * interface for #saveEventForCurrentUser
     */
    @FunctionalInterface
    public interface UserActivity {
        ActivityEventBuilder builderForCurrentUser(ActivityEventBuilder activityEventBuilder);
    }

    /**
     * Initializes a new event for the current user, allows the caller to specify the
     * action, target, ids, etc. for that event, then saves that event.
     * Example:
     *     activityLogger.saveEventForCurrentUser(event ->
     *                 event.created().record().withRecordType(mytype).withId(myid));
     * @param userActivity lambda that adds details to an ActivityEventBuilder
     */
    public void saveEventForCurrentUser(UserActivity userActivity) {
        saveEvent(userActivity.builderForCurrentUser(newEvent().currentUser()).build());
    }

    protected void saveEvent(ActivityEvent event) {
        if (LOGGER.isInfoEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("user %s %s".formatted(event.subject(), event.action().getName()));
            if (event.ids() != null) {
                sb.append(" %s".formatted(event.ids().length));
            } else if (event.quantity() != null) {
                sb.append(" %s".formatted(event.quantity()));
            }
            sb.append(" %s(s)".formatted(event.thing().getName()));
            if (event.recordType() != null) {
                sb.append(" of type %s".formatted(event.recordType().getName()));
            }
            if (event.ids() != null) {
                sb.append(" with id(s) %s".formatted(Arrays.toString(event.ids())));
            }
            LOGGER.info(sb.toString());
        }
    }


}
