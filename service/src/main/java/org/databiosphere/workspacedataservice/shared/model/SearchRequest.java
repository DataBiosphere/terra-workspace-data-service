package org.databiosphere.workspacedataservice.shared.model;

import java.util.List;

public class SearchRequest {

	private int limit = 10;
	private int offset = 0;
	private SortDirection sort = SortDirection.ASC;
	private String sortAttribute = null;

	private String filter = null; // global search filter

	private String filterOperator = "AND"; // global search operator; should be an enum

	private List<SearchColumn> searchColumnList;

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

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public String getFilterOperator() {
		return filterOperator;
	}

	public void setFilterOperator(String filterOperator) {
		this.filterOperator = filterOperator;
	}

	public SearchRequest(int limit, int offset, SortDirection sort, String sortAttribute, String filter, String filterOperator) {
		this.limit = limit;
		this.offset = offset;
		this.sort = sort;
		this.sortAttribute = sortAttribute;
		this.filter = filter;
		this.filterOperator = filterOperator;
	}

	public List<SearchColumn> getSearchColumnList() {
		return searchColumnList;
	}

	public void setSearchColumnList(List<SearchColumn> searchColumnList) {
		this.searchColumnList = searchColumnList;
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

	public String getSortAttribute() {
		return sortAttribute;
	}

	public void setSortAttribute(String sortAttribute) {
		this.sortAttribute = sortAttribute;
	}
}
