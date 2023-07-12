package org.databiosphere.workspacedataservice.activitylog;

public class ActivityModels {

    public enum Action {
        CREATE("created"), DELETE("deleted"),
        UPDATE("updated"), UPSERT("upserted"),
        MODIFY("modified"), LINK("linked"),
        RESTORE("restored");

        private final String name;

        public String getName() {
            return name;
        }

        Action(String name) {
            this.name = name;
        }
    }

    public enum Thing {
        INSTANCE("instance"), TABLE("table"),
        RECORD("record"), SNAPSHOT_REFERENCE("snapshot reference"),
        BACKUP("backup");

        private final String name;

        public String getName() {
            return name;
        }

        Thing(String name) {
            this.name = name;
        }
    }
}
