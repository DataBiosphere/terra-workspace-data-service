package org.databiosphere.workspacedataservice.activitylog;

import org.databiosphere.workspacedataservice.sam.SamDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class ActivityLogger {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActivityLogger.class);

    private final SamDao samDao;

    public ActivityLogger(SamDao samDao) {
        this.samDao = samDao;
    }


    public ActivityEventBuilder newEvent() {
        return new ActivityEventBuilder(this, this.samDao);
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
