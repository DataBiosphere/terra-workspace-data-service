package org.databiosphere.workspacedataservice.activitylog;

public class ActivityModels {

    public enum Action {
        CREATE("created"), DELETE("deleted"),
        UPDATE("updated"), UPSERT("upserted"),
        MODIFY("modified"), LINK("linked");

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
        RECORD("record"), SNAPSHOT_REFERENCE("snapshot reference");

        private final String name;

        public String getName() {
            return name;
        }

        Thing(String name) {
            this.name = name;
        }
    }







}
