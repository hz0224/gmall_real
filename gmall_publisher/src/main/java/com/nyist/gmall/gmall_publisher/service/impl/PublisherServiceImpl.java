package com.nyist.gmall.gmall_publisher.service.impl;

import com.google.gson.GsonBuilder;
import com.nyist.gmall.gmall_publisher.bean.DauHour;
import com.nyist.gmall.gmall_publisher.bean.OrderAmountHour;
import com.nyist.gmall.gmall_publisher.mapper.DauMapper;
import com.nyist.gmall.gmall_publisher.mapper.OrderMapper;
import com.nyist.gmall.gmall_publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    JestClient jestClient = getClient().getObject();

    @Override
    public long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauHourTotal(String date) {

        List<DauHour> dauHourTotal = dauMapper.getDauHourTotal(date);
        HashMap<String, Long> map = new HashMap<>();

        for (DauHour dauHour : dauHourTotal) {
            map.put(dauHour.getLogHour(),dauHour.getCt());
        }
        return map;
    }

    @Override
    public double getOrderAmountTotal(String date) {
        return orderMapper.getOrderAmountTotal(date);
    }

    @Override
    public Map<String, Object> getOrderAmountHourTotal(String date) {

        List<OrderAmountHour> orderAmountHourTotal = orderMapper.getOrderAmountHourTotal(date);
        HashMap<String, Object> map = new HashMap<>();

        for (OrderAmountHour orderAmountHour : orderAmountHourTotal) {
            map.put(orderAmountHour.getCreateHour(),orderAmountHour.getTotalAmount());
        }
        return map;
    }

    @Override
    public Map<String, Object> getSaleDetailFromES(String date, String keyword, int pageNo, int pagesize) {
        String query="{\n" +
                "  \"query\": {\n" +
                "    \n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"dt\": \"2019-08-18\"\n" +
                "        }\n" +
                "        \n" +
                "      }\n" +
                "      , \"must\": {\n" +
                "        \"match\":{\n" +
                "         \"sku_name\": {\n" +
                "             \"query\": \"小米高速双频\",\n" +
                "             \"operator\": \"and\"\n" +
                "         }\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "    \n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupby_gender\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"user_gender\",\n" +
                "        \"size\": 2\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "  ,\n" +
                "  \"from\": 0,   \n" +
                "  \"size\": 3\n" +
                "  \n" +
                "  \n" +
                "}";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //构造过滤、匹配条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date ));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合
        TermsAggregationBuilder genderAggs = AggregationBuilders.terms("groupby_gender").field("user_gender").size(2);
        TermsAggregationBuilder ageAggs = AggregationBuilders.terms("groupby_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(genderAggs);
        searchSourceBuilder.aggregation(ageAggs);
        //分页
        searchSourceBuilder.from( (pageNo-1)*pagesize   );
        searchSourceBuilder.size( pagesize   );

        Search search = new Search.Builder(searchSourceBuilder.toString()).build();

        Map<String,Object> resultMap=new HashMap<>();
        try {
            SearchResult searchResult = jestClient.execute(search);

            resultMap.put("total",searchResult.getTotal());  //总数

            List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
            List<Map>  saleList =new ArrayList<>();
            for (SearchResult.Hit<Map, Void> hit : hits) {
                saleList.add(hit.source) ;
            }
            resultMap.put("saleList",saleList);  //明细

            Map genderMap=new HashMap();
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_gender").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                genderMap.put( bucket.getKey(), bucket.getCount());
            }
            resultMap.put("genderMap",genderMap);  //性别聚合

            Map ageMap=new HashMap();
            List<TermsAggregation.Entry> agebuckets = searchResult.getAggregations().getTermsAggregation("groupby_age").getBuckets();
            for (TermsAggregation.Entry bucket : agebuckets) {
                ageMap.put( bucket.getKey(), bucket.getCount());
            }
            resultMap.put("ageMap",ageMap);  //年龄聚合

        } catch (IOException e) {
            e.printStackTrace();
        }
        return   resultMap;
    }

    public static JestClientFactory getClient(){
        JestClientFactory factory = new JestClientFactory();
        HttpClientConfig config = new HttpClientConfig
                .Builder("http://hangzhou1-yun:9200")
                .connTimeout(5000)
                .readTimeout(5000)
                .multiThreaded(true)
                .build();
        factory.setHttpClientConfig(config);
        return factory;
    }


}
