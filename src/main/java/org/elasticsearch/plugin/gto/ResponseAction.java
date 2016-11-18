package org.elasticsearch.plugin.gto;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.AnyGetterWriter;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by LJT on 16-11-17.
 * email: linjuntan@sensetime.com
 */
public class ResponseAction {

    /*

    * */
    /**
     *
     * @param client
     * @param channel
     * @param param
     */
    public void execute(Client client, RestChannel channel, String param) {
        RequestParam rp = JSON.parseObject(param, RequestParam.class);
        XContentBuilder builder = null;
        try {
            builder = XContentFactory.jsonBuilder()
                    .startObject()
                        .field("size", 0);
            if (rp.getQueryJson() != null) {
                builder.field("query", JSON.parseObject(rp.getQueryJson()).get("query"));
            }
            //JSONObject aggs = new JSONObject();
            //aggs.put("result", buildAggs(rp));
            builder.field("aggs", buildAggs(rp));
            SearchResponse sr = client.prepareSearch().setIndices(rp.getIndex()).setTypes(rp.getType())
                    .setSource(builder.string()).execute().actionGet();
            buildResponseInfo(sr, rp, channel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void buildResponseInfo(SearchResponse sr, RequestParam rp, RestChannel channel) {
        ResponseInfo info = new ResponseInfo();
        List<List<String>> results = new ArrayList<>();
        int index = rp.getNumber() * rp.getPage();
            Terms terms = sr.getAggregations().get("result");
            for (int j = index; j < terms.getBuckets().size(); j ++) {
                Terms.Bucket b = terms.getBuckets().get(j);
                TopHits topHits = b.getAggregations().get("topHits");
                SearchHit hits[] = topHits.getHits().getHits();
                List<String> strs = new ArrayList<>();
                for (int i = 0; i < hits.length; i++) {
                    strs.add(hits[i].getSourceAsString());
                }
                results.add(strs);
            }
        Double size = (Double) sr.getAggregations().get("cnt").getProperty("value");
        info.setCnt(size.intValue());
        info.setResults(results);
        BytesRestResponse brr = new BytesRestResponse(RestStatus.OK, JSON.toJSONString(info));
        channel.sendResponse(brr);
    }

/*    private void buildResponseInfo(SearchResponse sr, RequestParam rp) {
        ResponseInfo info = new ResponseInfo();
        List<List<String>> results = new ArrayList<>();
        int index = rp.getNumber() * rp.getPage();
*//*        if (rp.getSortMetric() == null && rp.getSortType() == null) {*//*
        Terms terms = sr.getAggregations().get("result");
        for (int j = index; j < terms.getBuckets().size(); j ++) {
            Terms.Bucket b = terms.getBuckets().get(j);
            TopHits topHits = b.getAggregations().get("topHits");
            SearchHit hits[] = topHits.getHits().getHits();
            List<String> strs = new ArrayList<>();
            for (int i = 0; i < hits.length; i++) {
                strs.add(hits[i].getSourceAsString());
            }
            results.add(strs);
        }
        Double size = (Double) sr.getAggregations().get("cnt").getProperty("value");
        info.setCnt(size.intValue());
        info.setResults(results);
        BytesRestResponse brr = new BytesRestResponse(RestStatus.OK, JSON.toJSONString(info));
        //channel.sendResponse(brr);
    }*/

/*    public void localTest(RequestParam rp) throws IOException {
        TransportClient client = TransportClient.builder().build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("es"), 9388));
        //RequestParam rp = JSON.parseObject(param, RequestParam.class);
        XContentBuilder builder = null;
        try {
            builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("size", 0);
            if (rp.getQueryJson() != null) {
                builder.field("query", JSON.parseObject(rp.getQueryJson()).get("query"));
            }
JSONObject aggs = new JSONObject();
            //aggs.put("result", buildAggs(rp));

            builder.field("aggs", buildAggs(rp));
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(builder.string());
        SearchResponse response = client.prepareSearch().setIndices(rp.getIndex()).setTypes(rp.getType())
                .setSource(builder.string()).execute().actionGet();
        int d = 5;
        System.out.println(response.toString());
        buildResponseInfo(response, rp);
    }*/


    private JSONObject buildAggs(RequestParam rp) throws Exception {
        JSONObject group = new JSONObject();
        JSONObject terms = new JSONObject();
        JSONObject subAggs = new JSONObject();
        JSONObject tsort = null;
        JSONObject topHits = new JSONObject();
        terms.put("size", (rp.getPage() + 1) * rp.getNumber());
        terms.put("field", rp.getGroupBy());
        if (rp.getSortMetric() == null && rp.getSortType() == null) {
            topHits.put("sort", JSON.parseArray(rp.getTopHitsOrderJson()));
            topHits.put("size", rp.getTopSize());
            //throw new Exception("without metric ");
        } else if (rp.getSortMetric() != null && rp.getSortType() != null) {
            JSONObject metric = new JSONObject();
            metric.put("field", rp.getSortMetric());
            tsort = new JSONObject();
            tsort.put(rp.getSortType(), metric);
            topHits.put("sort", JSON.parseArray(rp.getTopHitsOrderJson()));
            topHits.put("size", rp.getTopSize());
            subAggs.put("top_hits", topHits);
        } else {
            throw new Exception("without metric or type");
        }
        JSONObject aggsName = new JSONObject();
        if (tsort == null) {
            subAggs.put("top_hits", topHits);
            aggsName.put("topHits", subAggs);
            //terms.put("topHits", topHits);
        } else {
            //JSONObject aggsName = new JSONObject();
            aggsName.put("metricsort", tsort);
            aggsName.put("topHits", subAggs);
            JSONObject metricsort = new JSONObject();
            metricsort.put("metricsort", rp.getMetricOrderType());
            terms.put("order", metricsort);
        }
        group.put("terms", terms);
        group.put("aggs", aggsName);
        JSONObject all = new JSONObject();
        JSONObject cardinalityName = new JSONObject();
        JSONObject cardinality = new JSONObject();
        cardinality.put("field", rp.getGroupBy());
        cardinalityName.put("cardinality", cardinality);
        all.put("result", group);
        all.put("cnt", cardinalityName);
        return all;
    }

/*    public static void main(String []args) {
        RequestParam param = new RequestParam();
        param.setGroupBy("trackId");
        param.setMetricOrderType("desc");
        param.setSortMetric("time");
        param.setSortType("max");
        param.setTopHitsOrderJson("[{\"time\" : {\"order\": \"desc\"}}]");
        param.setTopSize(1);
        param.setPage(1);
        param.setNumber(15);
        param.setIndex("stkj_sf_rt_capture");
        param.setType("history");
        //param.setQueryJson();
        ResponseAction a = new ResponseAction();
        param = JSON.parseObject("{ \"index\" : \"stkj_sf_rt_capture\",         \"type\" : \"history\",         \"groupBy\" : \"trackId\",         \"sortMetric\" : \"time\",         \"sortType\" : \"max\",         \"number\" : 10,         \"page\" : 1,         \"topSize\": 1,         \"metricOrderType\" : \"desc\",         \"topHitsOrderJson\" : \"[{\\\"time\\\" : {\\\"order\\\": \\\"desc\\\"}}]\"     }", RequestParam.class);
        try {
            System.out.println(a.buildAggs(param).toJSONString());
            a.localTest(param);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/
}
