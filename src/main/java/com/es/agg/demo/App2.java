package com.es.agg.demo;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class App2<T> {

    private RestHighLevelClient client;

    /**
     * group by 多字段统计
     *
     * @param index
     * @param type
     * @param queryBuilder
     * @param clazz
     * @param groupKeys
     * @return
     */
    public List<T> group(String index, String type, String resultKey, BoolQueryBuilder queryBuilder, Class<T> clazz, String... groupKeys) {
        TermsAggregationBuilder firstBuilder = null;
        TermsAggregationBuilder topAggregationBuilder = null;
        for (String groupKey : groupKeys) {
            TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms(groupKey).field(groupKey);
            if (topAggregationBuilder != null) {
                topAggregationBuilder.subAggregation(aggregationBuilder);
            } else {
                firstBuilder = aggregationBuilder;
            }
            topAggregationBuilder = aggregationBuilder;
        }
        SearchResponse response = this.group(index, type, queryBuilder, firstBuilder);
        List<EsModel> finishModels = new ArrayList<>();
        Terms terms = (Terms) response.getAggregations().getAsMap().get(groupKeys[0]);
        this.buildEsModel(groupKeys, resultKey, terms, null, 0, finishModels);
        return finishModels.stream().map(item -> item.build(clazz)).collect(Collectors.toList());
    }

    /**
     * 构建es group by model
     *
     * @param groupKeys
     * @param terms
     * @param topEsModel
     * @param level
     * @param finishModels
     */
    private void buildEsModel(String[] groupKeys, String resultKey, Terms terms, EsModel topEsModel, int level, List<EsModel> finishModels) {
        if (level == (groupKeys.length - 1)) {
            if (terms != null && terms.getBuckets() != null && !terms.getBuckets().isEmpty()) {
                for (Terms.Bucket bucket : terms.getBuckets()) {
                    EsModel parentEsModel = new EsModel(groupKeys[level], bucket.getKey().toString(), topEsModel);
                    EsModel childEsModel = new EsModel(resultKey, bucket.getDocCount(), parentEsModel);
                    finishModels.add(childEsModel);
                }
            }
        } else {
            if (terms != null && terms.getBuckets() != null && !terms.getBuckets().isEmpty()) {
                for (Terms.Bucket bucket : terms.getBuckets()) {
                    EsModel esModel = new EsModel(groupKeys[level], bucket.getKey().toString(), topEsModel);
                    Terms childTerms = (Terms) bucket.getAggregations().getAsMap().get(groupKeys[level + 1]);
                    buildEsModel(groupKeys, resultKey, childTerms, esModel, level + 1, finishModels);
                }
            } else {
                return;
            }
        }
    }

    public SearchResponse group(String index, String type, BoolQueryBuilder queryBuilder, TermsAggregationBuilder aggregationBuilder) {
        // 搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.aggregation(aggregationBuilder);
        searchSourceBuilder.size(0);
        // 基础设置
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(type);
        searchRequest.source(searchSourceBuilder);
        try {
            return client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
