package com.es.agg.demo;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.HashMap;
import java.util.List;

public class App {

    public void demo1(RestHighLevelClient client) {
        // group
        TermsAggregationBuilder oneBuilder = AggregationBuilders.terms("one").field("flowCode.keyword");
        TermsAggregationBuilder twoBuilder = AggregationBuilders.terms("two").field("stepExecuteId");
        oneBuilder.subAggregation(twoBuilder);
        // search
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(oneBuilder);
        // request
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.types("type");
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            Terms oneTerm = (Terms) response.getAggregations().asMap().get("one");
            for (Terms.Bucket bucket : oneTerm.getBuckets()) {
                System.out.println("one下面的" + bucket.getKey() + ", count是: " + bucket.getDocCount());
                Terms twoTerm = (Terms) bucket.getAggregations().asMap().get("two");
                for (Terms.Bucket twoTermBucket : twoTerm.getBuckets()) {
                    System.out.println("two下面的" + twoTermBucket.getKey() + ", count是: " + twoTermBucket.getDocCount());
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void demo2(RestHighLevelClient client) {
        // script
        Script script = new Script(ScriptType.INLINE, "groovy", "doc['flowCode.keyword'].value+'-split-'+doc['stepExecuteId'].value", new HashMap<>());
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("result").script(script);
        // search
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(aggregationBuilder);
        // request
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.types("type");
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            Terms oneTerm = (Terms) response.getAggregations().asMap().get("result");
            for (Terms.Bucket bucket : oneTerm.getBuckets()) {
                // 此处自己split分隔处理
                System.out.println(bucket.getKey().toString());
                System.out.println(bucket.getDocCount());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
