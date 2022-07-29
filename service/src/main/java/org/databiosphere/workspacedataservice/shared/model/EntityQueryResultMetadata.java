package org.databiosphere.workspacedataservice.shared.model;

public class EntityQueryResultMetadata {

    private int unfilteredCount;

    private int filteredCount;

    private int filteredPageCount;

    public EntityQueryResultMetadata(int unfilteredCount, int filteredCount, int filteredPageCount) {
        this.unfilteredCount = unfilteredCount;
        this.filteredCount = filteredCount;
        this.filteredPageCount = filteredPageCount;
    }

    public EntityQueryResultMetadata() {
    }

    public int getUnfilteredCount() {
        return unfilteredCount;
    }

    public void setUnfilteredCount(int unfilteredCount) {
        this.unfilteredCount = unfilteredCount;
    }

    public int getFilteredCount() {
        return filteredCount;
    }

    public void setFilteredCount(int filteredCount) {
        this.filteredCount = filteredCount;
    }

    public int getFilteredPageCount() {
        return filteredPageCount;
    }

    public void setFilteredPageCount(int filteredPageCount) {
        this.filteredPageCount = filteredPageCount;
    }
}
