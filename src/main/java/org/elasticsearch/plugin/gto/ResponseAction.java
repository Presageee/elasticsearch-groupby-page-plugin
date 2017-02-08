package org.elasticsearch.plugin.gto;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.AnyGetterWriter;
import org.elasticsearch.action.search.SearchRequestBuilder;
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
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by LJT on 16-11-17.
 */
public class ResponseAction {
    /*

    * */

    /**
     * @param client
     * @param channel
     * @param param
     */
    public void execute(Client client, RestChannel channel, String param) {
        RequestParam rp = JSON.parseObject(param, RequestParam.class);
        //XContentBuilder builder = null;
        try {
            JSONObject json = new JSONObject();
            json.put("size", 0);
            if (rp.getQueryJson() != null) {
                json.put("query", JSON.parseObject(rp.getQueryJson()));
            }
            json.put("aggs", buildAggs(rp));
            SearchResponse sr = client.prepareSearch().setIndices(rp.getIndex()).setTypes(rp.getType())
                    .setSource(JSON.toJSONString(json)).execute().actionGet();
            System.out.println(">>>>>> get search result");
            buildResponseInfo(sr, rp, channel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void getCnt(SearchResponse response) {

    }

    private void buildResponseInfo(SearchResponse sr, RequestParam rp, RestChannel channel) {
        ResponseInfo info = new ResponseInfo();
        List<List<String>> results = new ArrayList<>();
        int index = rp.getNumber() * rp.getPage();
        Terms terms = sr.getAggregations().get("result");
        for (int j = index; j < terms.getBuckets().size(); j++) {
            Terms.Bucket b = terms.getBuckets().get(j);
            TopHits topHits = b.getAggregations().get("topHits");
            SearchHit hits[] = topHits.getHits().getHits();
            List<String> strs = new ArrayList<>();
            for (int i = 0; i < hits.length; i++) {
                JSONObject obj = JSON.parseObject(hits[i].getSourceAsString());
                obj.put("esId", hits[i].getId());
                strs.add(JSON.toJSONString(obj));
            }
            results.add(strs);
        }
        Double size = (Double) sr.getAggregations().get("cnt").getProperty("value");
        info.setCnt(size.intValue());
        info.setResults(results);
        System.out.println(">>>>>> size " + results.size());
        BytesRestResponse brr = new BytesRestResponse(RestStatus.OK, JSON.toJSONString(info));
        channel.sendResponse(brr);
    }

    private void buildRespInfo(SearchResponse sr, RequestParam rp) {
        ResponseInfo info = new ResponseInfo();
        List<List<String>> results = new ArrayList<>();
        int index = rp.getNumber() * rp.getPage();
        Terms terms = sr.getAggregations().get("result");
        for (int j = index; j < terms.getBuckets().size(); j++) {
            Terms.Bucket b = terms.getBuckets().get(j);
            TopHits topHits = b.getAggregations().get("topHits");
            SearchHit hits[] = topHits.getHits().getHits();
            List<String> strs = new ArrayList<>();
            for (int i = 0; i < hits.length; i++) {
                JSONObject obj = JSON.parseObject(hits[i].getSourceAsString());
                obj.put("esId", hits[i].getId());
                strs.add(JSON.toJSONString(obj));
            }
            results.add(strs);
        }
        Double size = (Double) sr.getAggregations().get("cnt").getProperty("value");
        info.setCnt(size.intValue());
        info.setResults(results);
        BytesRestResponse brr = new BytesRestResponse(RestStatus.OK, JSON.toJSONString(info));
    }

    public void localTest(RequestParam rp) throws IOException {
        TransportClient client = TransportClient.builder().build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("elasticsearch"), 9388));
        //RequestParam rp = JSON.parseObject(param, RequestParam.class);
/*        XContentBuilder builder = null;
        try {
            builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("size", 0);
            if (rp.getQueryJson() != null) {
                builder.field("query", JSON.parseObject(rp.getQueryJson()));
            }
            //JSONObject aggs = new JSONObject();
            //aggs.put("result", buildAggs(rp));

            builder.field("aggs", buildAggs(rp));
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(builder.string());*/
        JSONObject json = new JSONObject();
        json.put("size", 0);
        if (rp.getQueryJson() != null) {
            json.put("query", JSON.parseObject(rp.getQueryJson()));
        }
        try {
            json.put("aggs", buildAggs(rp));
        } catch (Exception e) {
            e.printStackTrace();
        }
        SearchResponse response = client.prepareSearch().setIndices(rp.getIndex()).setTypes(rp.getType())
                .setSource(JSON.toJSONString(json)).execute().actionGet();
        buildRespInfo(response, rp);
        int d = 5;
        System.out.println(response.toString());
        //buildResponseInfo(response, rp);
    }

    public JSONObject buildCntAggs(RequestParam rp) throws Exception{
        JSONObject group = new JSONObject();
        JSONObject terms = new JSONObject();
        JSONObject cardinalityName = new JSONObject();
        JSONObject cardinality = new JSONObject();
        cardinality.put("field", rp.getGroupBy());
        cardinalityName.put("cardinality", cardinality);
        terms.put("cnt", cardinalityName);
        return terms;
    }

    private JSONObject buildAggs(RequestParam rp) throws Exception {
        JSONObject group = new JSONObject();
        JSONObject terms = new JSONObject();
        JSONObject subAggs = new JSONObject();
        JSONObject tsort = null;
        JSONObject topHits = new JSONObject();
        int size = (rp.getPage() + 1) * rp.getNumber();
        terms.put("size", size == 0 ? 1 : size);
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

    public static void main(String[] args) throws IOException {
        ResponseAction responseAction = new ResponseAction();
        //RequestParam rp = JSON.parseObject("{\"index\":\"stkj_sf_rt_capture\",\"type\":\"history\",\"queryJson\":\"{\\n  \\\"and\\\" : {\\n    \\\"filters\\\" : [ {\\n      \\\"match\\\" : {\\n        \\\"taskSerial\\\" : {\\n          \\\"query\\\" : \\\"1158\\\",\\n          \\\"type\\\" : \\\"boolean\\\"\\n        }\\n      }\\n    }, {\\n      \\\"match\\\" : {\\n        \\\"videoSerial\\\" : {\\n          \\\"query\\\" : \\\"65ee87db33b24450b4bca04a9bda4f91\\\",\\n          \\\"type\\\" : \\\"boolean\\\"\\n        }\\n      }\\n    }, {\\n      \\\"range\\\" : {\\n        \\\"time\\\" : {\\n          \\\"from\\\" : 1478016000000,\\n          \\\"to\\\" : 1479743999000,\\n          \\\"include_lower\\\" : false,\\n          \\\"include_upper\\\" : false\\n        }\\n      }\\n    } ]\\n  }\\n}\",\"groupBy\":\"trackId\",\"sortMetric\":\"time\",\"sortType\":\"max\",\"topSize\":1,\"topHitsOrderJson\":\"[{\\\"time\\\":{\\\"order\\\":\\\"desc\\\"}}]\",\"metricOrderType\":\"desc\",\"number\":240,\"page\":0}", RequestParam.class);
        //RequestParam rp = JSON.parseObject("{\"index\":\"stkj_sf_rt_capture\",\"type\":\"history\",\"groupBy\":\"trackId\",\"sortMetric\":\"time\",\"sortType\":\"max\",\"topSize\":1,\"topHitsOrderJson\":\"[{\\\"time\\\":{\\\"order\\\":\\\"desc\\\"}}]\",\"metricOrderType\":\"desc\",\"number\":240,\"page\":0,\"queryJson\":\"{\\n  \\\"and\\\" : {\\n    \\\"filters\\\" : [ {\\n      \\\"match\\\" : {\\n        \\\"taskSerial\\\" : {\\n          \\\"query\\\" : \\\"1318\\\",\\n          \\\"type\\\" : \\\"boolean\\\"\\n        }\\n      }\\n    }, {\\n      \\\"match\\\" : {\\n        \\\"videoSerial\\\" : {\\n          \\\"query\\\" : \\\"5eb35b6e95c14854a3eaead34ca6809d\\\",\\n          \\\"type\\\" : \\\"boolean\\\"\\n        }\\n      }\\n    }, {\\n      \\\"range\\\" : {\\n        \\\"time\\\" : {\\n          \\\"from\\\" : 1479830400000,\\n          \\\"to\\\" : 1480694399000,\\n          \\\"include_lower\\\" : true,\\n          \\\"include_upper\\\" : true\\n        }\\n      }\\n    } ]\\n  }\\n}\"}", RequestParam.class);
        RequestParam rp = JSON.parseObject("{\"index\":\"stkj_sf_rt_capture\",\"type\":\"history\",\"groupBy\":\"taskVideoTrackSerial\",\"sortMetric\":\"time\",\"sortType\":\"max\",\"topSize\":1,\"topHitsOrderJson\":\"[{\\\"time\\\":{\\\"order\\\":\\\"desc\\\"}}]\",\"metricOrderType\":\"desc\",\"number\":240,\"page\":0,\"queryJson\":\"{\\n  \\\"and\\\" : {\\n    \\\"filters\\\" : [ {\\n      \\\"match\\\" : {\\n        \\\"taskSerial\\\" : {\\n          \\\"query\\\" : \\\"5\\\",\\n          \\\"type\\\" : \\\"boolean\\\"\\n        }\\n      }\\n    }, {\\n      \\\"or\\\" : {\\n        \\\"filters\\\" : [ {\\n          \\\"bool\\\" : {\\n            \\\"must\\\" : {\\n              \\\"match\\\" : {\\n                \\\"videoSerial\\\" : {\\n                  \\\"query\\\" : \\\"d48c26c29c3c47288ca6370ad84adacb\\\",\\n                  \\\"type\\\" : \\\"boolean\\\"\\n                }\\n              }\\n            }\\n          }\\n        }, {\\n          \\\"bool\\\" : {\\n            \\\"must\\\" : {\\n              \\\"match\\\" : {\\n                \\\"videoSerial\\\" : {\\n                  \\\"query\\\" : \\\"03eeddda94ea46f9a8db6c463833b063\\\",\\n                  \\\"type\\\" : \\\"boolean\\\"\\n                }\\n              }\\n            }\\n          }\\n        } ]\\n      }\\n    }, {\\n      \\\"range\\\" : {\\n        \\\"time\\\" : {\\n          \\\"from\\\" : 1484841600000,\\n          \\\"to\\\" : 1484927999000,\\n          \\\"include_lower\\\" : true,\\n          \\\"include_upper\\\" : true\\n        }\\n      }\\n    } ]\\n  }\\n}\"}", RequestParam.class);
        responseAction.localTest(rp);
    }
}