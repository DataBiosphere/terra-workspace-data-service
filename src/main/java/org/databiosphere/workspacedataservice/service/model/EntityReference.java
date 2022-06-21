package org.databiosphere.workspacedataservice.service.model;

public class EntityReference {

    private final String entityName;

    private final long entityType;

    private final long referencedEntityType;

    private final String referencedEntityName;

    public EntityReference(String entityName, long entityType, long referencedEntityType, String referencedEntityName) {
        this.entityName = entityName;
        this.entityType = entityType;
        this.referencedEntityType = referencedEntityType;
        this.referencedEntityName = referencedEntityName;
    }

    public String getEntityName() {
        return entityName;
    }

    public long getEntityType() {
        return entityType;
    }

    public long getReferencedEntityType() {
        return referencedEntityType;
    }

    public String getReferencedEntityName() {
        return referencedEntityName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EntityReference)) return false;

        EntityReference that = (EntityReference) o;

        if (getEntityType() != that.getEntityType()) return false;
        if (getReferencedEntityType() != that.getReferencedEntityType()) return false;
        if (!getEntityName().equals(that.getEntityName())) return false;
        return getReferencedEntityName().equals(that.getReferencedEntityName());
    }

    @Override
    public int hashCode() {
        int result = getEntityName().hashCode();
        result = 31 * result + (int) (getEntityType() ^ (getEntityType() >>> 32));
        result = 31 * result + (int) (getReferencedEntityType() ^ (getReferencedEntityType() >>> 32));
        result = 31 * result + getReferencedEntityName().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "EntityReference{" +
                "referencedEntityName='" + referencedEntityName + '\'' +
                '}';
    }
}
