package org.databiosphere.workspacedataservice.activitylog;

public class ActivityModels {

    public enum Action {
        CREATE("created"), DELETE("deleted"), UPDATE("updated"), UPSERT("upserted");

        private final String name;

        public String getName() {
            return name;
        }

        Action(String name) {
            this.name = name;
        }
    }

    public enum Thing {
        INSTANCE("instance"), TABLE("table"), RECORD("record");

        private final String name;

        public String getName() {
            return name;
        }

        Thing(String name) {
            this.name = name;
        }
    }







}
