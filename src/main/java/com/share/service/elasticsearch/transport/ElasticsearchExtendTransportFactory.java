package com.share.service.elasticsearch.transport;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.alibaba.fastjson.JSON;
import com.share.util.StringUtil;

public class ElasticsearchExtendTransportFactory extends ElasticsearchTransportFactory{
	public ElasticsearchExtendTransportFactory() {
		super();
	}
	public ElasticsearchExtendTransportFactory(String servers) {
		super(servers);
	}
	public ElasticsearchExtendTransportFactory(String servers,int port) {
		super(servers, port);
	}
	public ElasticsearchExtendTransportFactory(String clusterName, String servers,int port) {
		super(clusterName, servers, port);
	}
	public ElasticsearchExtendTransportFactory(String clusterName, String servers, String username, String password) {
		super(clusterName, servers, username, password);
	}
	public ElasticsearchExtendTransportFactory(String clusterName, String servers, String username, String password,int port) {
		super(clusterName, servers, username, password, port);
	}
	public String selectTermAll(String indexs,String types,String field,String value){
		try {
			if(client==null){
				init();
			}
			SearchRequestBuilder request = client.prepareSearch(indexs.split(",")).setTypes(types.split(","));
			request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.setQuery(QueryBuilders.termQuery(field, value));
			request.highlighter(new HighlightBuilder().field(field));
			request.addAggregation(AggregationBuilders.terms("data").field(field+".keyword"));
			request.setExplain(false);
			SearchResponse response = request.get();
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String selectTermAll(String indexs,String types,Map<String,Object> must,Map<String,Object> should,Map<String,Object> must_not,Map<String,List<Object>> ranges){
		try {
			if(client==null){
				init();
			}
			BoolQueryBuilder boolquery = QueryBuilders.boolQuery();
			HighlightBuilder highlight = new HighlightBuilder();
			if(must!=null&&must.size()>0){
				for (String field : must.keySet()) {
					if(field.matches(regex)){
						continue;
					}
					Object text = must.get(field);
					String value = text instanceof String ?text.toString():JSON.toJSONString(text);
					if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
						if(value.startsWith("[")&&value.endsWith("]")){
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<String> values = JSON.parseArray(value, String.class);
							for (String _value : values) {
								if(!_value.matches(regex)){
									child.should(QueryBuilders.termQuery(field, value));
								}
							}
							boolquery.must(child);
						}else{
							if(!value.matches(regex)){
								boolquery.must(QueryBuilders.termQuery(field, value));
							}
						}
					}
					highlight.field(field);
				}
			}
			if(should!=null&&should.size()>0){
				for (String field : should.keySet()) {
					if(field.matches(regex)){
						continue;
					}
					Object text = must.get(field);
					String value = text instanceof String ?text.toString():JSON.toJSONString(text);
					if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
						if(value.startsWith("[")&&value.endsWith("]")){
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<String> values = JSON.parseArray(value, String.class);
							for (String _value : values) {
								if(!_value.matches(regex)){
									child.should(QueryBuilders.termQuery(field, value));
								}
							}
							boolquery.should(child);
						}else{
							if(!value.matches(regex)){
								boolquery.should(QueryBuilders.termQuery(field, value));
							}
						}
					}
					highlight.field(field);
				}
			}
			if(must_not!=null&&must_not.size()>0){
				for (String field : must_not.keySet()) {
					if(field.matches(regex)){
						continue;
					}
					Object text = must.get(field);
					String value = text instanceof String ?text.toString():JSON.toJSONString(text);
					if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
						if(value.startsWith("[")&&value.endsWith("]")){
							BoolQueryBuilder child = QueryBuilders.boolQuery();
							List<String> values = JSON.parseArray(value, String.class);
							for (String _value : values) {
								if(!_value.matches(regex)){
									child.should(QueryBuilders.termQuery(field, value));
								}
							}
							boolquery.mustNot(child);
						}else{
							if(!value.matches(regex)){
								boolquery.mustNot(QueryBuilders.termQuery(field, value));
							}
						}
					}
					highlight.field(field);
				}
			}
			if(ranges!=null&&ranges.size()>0){
				for (String key : ranges.keySet()) {
					if(key.matches(regex)){
						continue;
					}
					List<Object> between = ranges.get(key);
					if(between!=null&&!between.isEmpty()){
						Object start = between.get(0);
						Object end = between.size()>1?between.get(1):null;
						if(start!=null&&end!=null){
							Long starttime = start instanceof Date?((Date)start).getTime():Long.valueOf(start.toString());
							Long endtime = end instanceof Date?((Date)end).getTime():Long.valueOf(end.toString());
							if(starttime>endtime){
								Object temp = start;
								start = end;
								end = temp;
							}
						}
						RangeQueryBuilder range = QueryBuilders.rangeQuery(key);
						if(start!=null){
							range.gte(start);
						}
						if(start!=null){
							range.lt(start);
						}
						boolquery.must(range);
					}
				}
			}
			SearchRequestBuilder request = client.prepareSearch(indexs.split(",")).setTypes(types.split(","));
			request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.setQuery(boolquery);
			request.highlighter(highlight);
			request.setExplain(false);
			SearchResponse response = request.get();
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String selectTermAll(String indexs, String types, Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges, String order, boolean isAsc, int pageNo,int pageSize) {
		if(client==null){
			init();
		}
		pageNo=pageNo<1?1:pageNo;
		pageSize=pageSize<1?10:pageSize;
		BoolQueryBuilder boolquery = QueryBuilders.boolQuery();
		HighlightBuilder highlight = new HighlightBuilder();
		if(must!=null&&must.size()>0){
			for (String field : must.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						BoolQueryBuilder child = QueryBuilders.boolQuery();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								child.should(QueryBuilders.termQuery(field, value));
							}
						}
						boolquery.must(child);
					}else{
						if(!value.matches(regex)){
							boolquery.must(QueryBuilders.termQuery(field, value));
						}
					}
				}
				highlight.field(field);
			}
		}
		if(should!=null&&should.size()>0){
			for (String field : should.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						BoolQueryBuilder child = QueryBuilders.boolQuery();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								child.should(QueryBuilders.termQuery(field, value));
							}
						}
						boolquery.should(child);
					}else{
						if(!value.matches(regex)){
							boolquery.should(QueryBuilders.termQuery(field, value));
						}
					}
				}
				highlight.field(field);
			}
		}
		if(must_not!=null&&must_not.size()>0){
			for (String field : must_not.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						BoolQueryBuilder child = QueryBuilders.boolQuery();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								child.should(QueryBuilders.termQuery(field, value));
							}
						}
						boolquery.mustNot(child);
					}else{
						if(!value.matches(regex)){
							boolquery.mustNot(QueryBuilders.termQuery(field, value));
						}
					}
				}
				highlight.field(field);
			}
		}
		if(ranges!=null&&ranges.size()>0){
			for (String key : ranges.keySet()) {
				if(key.matches(regex)){
					continue;
				}
				List<Object> between = ranges.get(key);
				if(between!=null&&!between.isEmpty()){
					Object start = between.get(0);
					Object end = between.size()>1?between.get(1):null;
					if(start!=null&&end!=null){
						Long starttime = start instanceof Date?((Date)start).getTime():Long.valueOf(start.toString());
						Long endtime = end instanceof Date?((Date)end).getTime():Long.valueOf(end.toString());
						if(starttime>endtime){
							Object temp = start;
							start = end;
							end = temp;
						}
					}
					RangeQueryBuilder range = QueryBuilders.rangeQuery(key);
					if(start!=null){
						range.gte(start);
					}
					if(start!=null){
						range.lt(start);
					}
					boolquery.must(range);
				}
			};
		}
		
		SearchRequestBuilder request = client.prepareSearch(indexs.split(",")).setTypes(types.split(","));
		request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
		request.setQuery(boolquery);
		request.highlighter(highlight);
		request.addSort(SortBuilders.scoreSort());
		if(!StringUtil.isEmpty(order)){
			request.addSort(SortBuilders.fieldSort(order).order(isAsc?SortOrder.ASC:SortOrder.DESC));
		}
		request.setFrom((pageNo-1)*pageSize);
		request.setSize(pageSize);
		request.setExplain(false);
		SearchResponse response = request.get();
		return response.toString();
	}
}
