package org.elasticsearch.plugin.gto;

/**
 * Created by LJT on 16-11-17.
 */
public class RequestParam {
/*    {
        "index" : "",
            "type" : "",
            "queryJson" : "",
            "gourpBy" : "",
            "sortMetric" : "",
            "sortType" : "",
            "topSize" : "",
            "topHitsOrderJson" : ""
            "number" : "",
            "page" : "",
    }*/
    private String index;
    private String type;
    private String queryJson;
    private String groupBy;
    private String sortMetric;
    private String sortType;
    private Integer number;
    private Integer page;
    private Integer topSize;
    private String topHitsOrderJson;
    private String metricOrderType;

    public String getMetricOrderType() {
        return metricOrderType;
    }

    public void setMetricOrderType(String metricOrderType) {
        this.metricOrderType = metricOrderType;
    }

    public String getTopHitsOrderJson() {
        return topHitsOrderJson;
    }

    public void setTopHitsOrderJson(String topHitsOrderJson) {
        this.topHitsOrderJson = topHitsOrderJson;
    }

    public Integer getTopSize() {
        return topSize;
    }

    public void setTopSize(Integer topSize) {
        this.topSize = topSize;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getQueryJson() {
        return queryJson;
    }

    public void setQueryJson(String queryJson) {
        this.queryJson = queryJson;
    }

    public String getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(String groupBy) {
        this.groupBy = groupBy;
    }

    public String getSortMetric() {
        return sortMetric;
    }

    public void setSortMetric(String sortMetric) {
        this.sortMetric = sortMetric;
    }

    public String getSortType() {
        return sortType;
    }

    public void setSortType(String sortType) {
        this.sortType = sortType;
    }

    public Integer getNumber() {
        return number;
    }

    public void setNumber(Integer number) {
        this.number = number;
    }

    public Integer getPage() {
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }
}
