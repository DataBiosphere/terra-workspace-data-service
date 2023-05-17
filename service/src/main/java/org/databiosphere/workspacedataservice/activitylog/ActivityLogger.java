package org.databiosphere.workspacedataservice.activitylog;

import org.databiosphere.workspacedataservice.sam.SamDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActivityLogger {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActivityLogger.class);

    private SamDao samDao;

    public ActivityLogger(SamDao samDao) {
        this.samDao = samDao;
    }


    public ActivityEventBuilder newEvent() {
        return new ActivityEventBuilder(this, this.samDao);
    }


    protected void saveEvent(ActivityEvent event) {
        LOGGER.info("user {} {} {} {}(s) with ids {}", event.subject(), event.action().getName(),
                event.ids().length, event.thing().getName(),
                event.ids());
    }


}
