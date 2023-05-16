package org.databiosphere.workspacedataservice.activitylog;

import org.databiosphere.workspacedataservice.sam.SamDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.RequestContextHolder;

import java.util.Objects;

import static org.databiosphere.workspacedataservice.sam.BearerTokenFilter.ATTRIBUTE_NAME_TOKEN;
import static org.springframework.web.context.request.RequestAttributes.SCOPE_REQUEST;

public class ActivityLogger {

    private static final Logger LOGGER = LoggerFactory.getLogger(ActivityLogger.class);

    private SamDao samDao;

    public ActivityLogger(SamDao samDao) {
        this.samDao = samDao;
    }


    public ActivityEventBuilder newEvent() {
        return new ActivityEventBuilder(this, this.samDao);
    }

//    public void saveInstanceActivity(ActivityModels.Action action, String id) {
//        saveActivity(action, 1L, ActivityModels.Thing.INSTANCE, new String[]{id});
//    }

//    private void saveActivity(ActivityModels.Action action, Long quantity, ActivityModels.Thing thing, String[] ids) {
//
//        // grab the current user's bearer token (see BearerTokenFilter)
//        Object token = RequestContextHolder.currentRequestAttributes()
//                .getAttribute(ATTRIBUTE_NAME_TOKEN, SCOPE_REQUEST);
//
//        // resolve the token to a user id via Sam
//        String userId;
//        if (!Objects.isNull(token)) {
//            LOGGER.debug("setting access token for data repo request");
//            userId = samDao.getUserId(token.toString());
//        } else {
//            LOGGER.warn("No access token found for data repo request.");
//            userId = "???";
//        }
//        // TODO: error handling for the Sam call; we should always log
//        ActivityEvent event = new ActivityEventBuilder().setSubject(userId).setAction(action).setQuantity(quantity).setThing(thing).setIds(ids).createActivityEvent();
//        saveActivity(event);
//    }

    protected void saveActivity(ActivityEvent event) {
        LOGGER.info("user {} {} {} {}(s) with ids {}", event.getSubject(), event.getAction().getName(),
                event.getQuantity(), event.getThing().getName(),
                event.getIds());
    }


}
