package org.databiosphere.workspacedataservice.shared.model;

public class EntityQueryParameters {

    private int page;

    private int pageSize;

    private String sortField;

    private String sortDirection;

    private String filterTerms;


    public EntityQueryParameters() {
    }

    public EntityQueryParameters(int page, int pageSize, String sortField, String sortDirection, String filterTerms) {
        this.page = page;
        this.pageSize = pageSize;
        this.sortField = sortField;
        this.sortDirection = sortDirection;
        this.filterTerms = filterTerms;
    }

    public int getPage() {
        return page;
    }

    public void setPage(int page) {
        this.page = page;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public String getSortField() {
        return sortField;
    }

    public void setSortField(String sortField) {
        this.sortField = sortField;
    }

    public String getSortDirection() {
        return sortDirection;
    }

    public void setSortDirection(String sortDirection) {
        this.sortDirection = sortDirection;
    }

    public String getFilterTerms() {
        return filterTerms;
    }

    public void setFilterTerms(String filterTerms) {
        this.filterTerms = filterTerms;
    }
}
