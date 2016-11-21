# elasticsearch-groupby-page-plugin

## GET http://ip:port/groupBy?req={jsonstring}
## POST http://ip:port/groupBy?req={jsonstring}

### JsonString
''

    {
            "index" : "",//index name
            "type" : "",//type name
            "queryJson" : "",//queryBuilders.toString
            "groupBy" : "",//terms field
            "sortMetric" : "",//sort metric
            "sortType" : "",//such as,max,min,avg...
            "topSize" : "",//topHits size
            "topHitsOrderJson" : "[{\"time\" : {\"order\": \"desc\"}}]"//topHitsOrderJsonArray
            "metricOrderType" : ""//desc asc
            "number" : "",//page size
            "page" : "",//page index
    }
''