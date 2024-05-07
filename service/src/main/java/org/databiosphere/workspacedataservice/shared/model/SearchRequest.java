package org.databiosphere.workspacedataservice.shared.model;

import java.util.Optional;
import org.springframework.lang.Nullable;

public class SearchRequest {

  private int limit = 10;
  private int offset = 0;
  private SortDirection sort = SortDirection.ASC;
  @Nullable private String sortAttribute = null;
  private Optional<SearchFilter> filter = Optional.empty();

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

  public SearchRequest() {}

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

  @Nullable
  public String getSortAttribute() {
    return sortAttribute;
  }

  public void setSortAttribute(String sortAttribute) {
    this.sortAttribute = sortAttribute;
  }

  public Optional<SearchFilter> getFilter() {
    return filter;
  }

  public void setFilter(
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<SearchFilter> filter) {
    this.filter = filter;
  }
}
