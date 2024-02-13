package org.databiosphere.workspacedataservice.activitylog;

public class ActivityModels {

  public enum Action {
    CREATE("created"),
    DELETE("deleted"),
    UPDATE("updated"),
    UPSERT("upserted"),
    MODIFY("modified"),
    LINK("linked"),
    RENAME("renamed"),
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
    COLLECTION("collection"),
    TABLE("table"),
    ATTRIBUTE("attribute"),
    RECORD("record"),
    SNAPSHOT_REFERENCE("snapshot reference"),
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
