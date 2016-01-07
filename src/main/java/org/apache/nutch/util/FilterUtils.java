package org.apache.nutch.util;

import org.apache.gora.filter.FilterOp;
import org.apache.gora.filter.MapFieldValueFilter;
import org.apache.gora.filter.SingleFieldValueFilter;
import org.apache.nutch.storage.Link;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;

import static org.apache.nutch.metadata.Nutch.ALL_CRAWL_ID;

/**
 *
 * @author Pierre Sutra
 */
public class FilterUtils {

  public static MapFieldValueFilter<String, WebPage> getBatchIdFilter(
    String batchId, Mark mark) {
    if ( batchId.equals(ALL_CRAWL_ID)) {
      return null;
    }
    MapFieldValueFilter<String, WebPage> filter = new MapFieldValueFilter<>();
    filter.setFieldName(WebPage.Field.MARKERS.getName());
    filter.setFilterOp(FilterOp.EQUALS);
    filter.setFilterIfMissing(true);
    filter.setMapKey(mark.getName());
    filter.getOperands().add(batchId);
    return filter;
  }

  public static MapFieldValueFilter<String, WebPage> getFetchedFilter(){
    MapFieldValueFilter<String, WebPage> filter = new MapFieldValueFilter<>();
    filter.setFieldName(WebPage.Field.MARKERS.getName());
    filter.setFilterOp(FilterOp.LIKE);
    filter.setFilterIfMissing(false);
    filter.setMapKey(Mark.FETCH_MARK.getName());
    filter.getOperands().add("*");
    return filter;
  }


  public static SingleFieldValueFilter<String, Link> getBatchIdLinkFilter(String batchId) {
    SingleFieldValueFilter<String, Link> filter = new SingleFieldValueFilter<>();
    filter.setFieldName("batchId");
    filter.setFilterOp(FilterOp.EQUALS);
    filter.getOperands().add(batchId);
    return filter;
  }
  
  public static MapFieldValueFilter<String, WebPage> getExcludeNonGeneratedFilter() {
    MapFieldValueFilter<String, WebPage> filter = new MapFieldValueFilter<>();
    filter.setFieldName(WebPage.Field.MARKERS.getName());
    filter.setFilterOp(FilterOp.LIKE);
    filter.setFilterIfMissing(false);
    filter.setMapKey(Mark.GENERATE_MARK.getName());
    filter.getOperands().add("*");
    return filter;
  }

  public static SingleFieldValueFilter<String, WebPage> getShouldFetchFilter(long time) {
    SingleFieldValueFilter<String, WebPage> filter = new SingleFieldValueFilter<>();
    filter.setFieldName(WebPage.Field.FETCH_TIME.getName());
    filter.setFilterOp(FilterOp.LESS_OR_EQUAL);
    filter.setFilterIfMissing(false);
    filter.getOperands().add(time);
    return filter;
  }
  
}
