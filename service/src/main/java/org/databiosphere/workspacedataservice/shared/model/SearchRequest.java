package org.databiosphere.workspacedataservice.shared.model;

public class SearchRequest {

	private int limit = 10;
	private int offset = 0;
	private SortDirection sort = SortDirection.ASC;
	private String sortAttribute = null;

	public SearchRequest(int limit, int offset, SortDirection sort) {
		this.limit = limit;
		this.offset = offset;
		this.sort = sort;
	}

	public SearchRequest(int limit, int offset, SortDirection sort, String sortAttribute) {
		this.limit = limit;
		this.offset = offset;
		this.sort = sort;
		this.sortAttribute = sortAttribute;
	}

	public SearchRequest() {
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public SortDirection getSort() {
		return sort;
	}

	public void setSort(SortDirection sort) {
		this.sort = sort;
	}

	public String getSortAttribute() { return sortAttribute; }

	public void setSortAttribute(String sortAttribute) { this.sortAttribute = sortAttribute; }
}
