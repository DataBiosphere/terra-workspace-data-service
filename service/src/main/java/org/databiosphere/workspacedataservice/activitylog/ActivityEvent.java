package org.databiosphere.workspacedataservice.activitylog;

public class ActivityEvent {



    private final String subject;
    private final ActivityModels.Action action;
    private final Long quantity;
    private final ActivityModels.Thing thing;
    private final String[] ids;

    public ActivityEvent(String subject, ActivityModels.Action action, Long quantity, ActivityModels.Thing thing, String[] ids) {
        this.subject = subject;
        this.action = action;
        this.quantity = quantity;
        this.thing = thing;
        this.ids = ids;
    }

    public String getSubject() {
        return subject;
    }

    public ActivityModels.Action getAction() {
        return action;
    }

    public Long getQuantity() {
        return quantity;
    }

    public ActivityModels.Thing getThing() {
        return thing;
    }

    public String[] getIds() {
        return ids;
    }
}
