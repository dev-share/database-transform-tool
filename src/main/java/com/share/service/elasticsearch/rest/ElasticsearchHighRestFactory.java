package com.share.service.elasticsearch.rest;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.share.util.StringUtil;

public class ElasticsearchHighRestFactory extends ElasticsearchRestFactory{
	private static Logger logger = LogManager.getLogger();
	protected RestHighLevelClient xclient=null;
	
	public ElasticsearchHighRestFactory() {
		super();
	}
	public ElasticsearchHighRestFactory(String servers) {
		super(servers);
	}
	public ElasticsearchHighRestFactory(String servers,int port) {
		super(servers, port);
	}
	public ElasticsearchHighRestFactory(String clusterName, String servers,int port) {
		super(clusterName, servers, port);
	}
	public ElasticsearchHighRestFactory(String clusterName, String servers, String username, String password) {
		super(clusterName, servers, username, password);
	}
	public ElasticsearchHighRestFactory(String clusterName, String servers, String username, String password,int port) {
		super(clusterName, servers, username, password, port);
	}

	/**
	 * 描述: Elasticsearch服务初始化
	 * 时间: 2017年11月14日 上午10:55:02
	 * @author yi.zhang
	 */
	public void init(){
		try {
			super.init();
			xclient = new RestHighLevelClient(builder);
		} catch (Exception e) {
			logger.error("-----Elasticsearch Config init Error-----", e);
		}
	}
	
	public RestHighLevelClient getXClient(){
		return xclient;
	}
	
	public String insert(String index,String type,Object json){
		try {
			if(xclient==null){
				init();
			}
//			XContentBuilder builder = XContentFactory.jsonBuilder();
//			builder.startObject();
//			{
//			    builder.field("user", "kimchy");
//			    builder.field("postDate", new Date());
//			    builder.field("message", "trying out Elasticsearch");
//			}
//			builder.endObject();
			IndexRequest request = new IndexRequest(index, type);
			request.source(JSON.parseObject(JSON.toJSONString(json)),XContentType.JSON);
			IndexResponse response = xclient.index(request);
//			String _index = response.getIndex();
//			String _type = response.getType();
//			String id = response.getId();
//			long version = response.getVersion();
//			if (response.getResult() == DocWriteResponse.Result.CREATED) {
//			    
//			} else if (response.getResult() == DocWriteResponse.Result.UPDATED) {
//			    
//			}
//			ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
//			if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
//			    
//			}
//			if (shardInfo.getFailed() > 0) {
//			    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
//			        String reason = failure.reason(); 
//			    }
//			}
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String update(String index,String type,String id,Object json){
		try {
			if(xclient==null){
				init();
			}
			UpdateRequest request = new UpdateRequest(index, type, id);
			request.doc(JSON.parseObject(JSON.toJSONString(json)),XContentType.JSON);
			UpdateResponse response = xclient.update(request);
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String upsert(String index,String type,String id,Object json){
		try {
			if(xclient==null){
				init();
			}
//			IndexRequest indexRequest = new IndexRequest(index, type, id).source(json,XContentType.JSON);
//			UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(json,XContentType.JSON).upsert(indexRequest);
			UpdateRequest request = new UpdateRequest(index, type, id);
			request.upsert(JSON.parseObject(JSON.toJSONString(json)),XContentType.JSON);
			UpdateResponse response = xclient.update(request);
			return response.toString();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String delete(String index,String type,String id){
		try {
			if(xclient==null){
				init();
			}
			DeleteRequest request = new DeleteRequest(index, type, id);
			DeleteResponse result = xclient.delete(request);
			return result.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String bulkUpsert(String index,String type,List<Object> jsons){
		try {
			if(xclient==null){
				init();
			}
			BulkRequest request = new BulkRequest();
			for (Object json : jsons) {
				JSONObject obj = JSON.parseObject(JSON.toJSONString(json));
				String id = UUIDs.base64UUID();
				if(obj.containsKey("id")){
					id = obj.getString("id");
					obj.remove("id");
				}
//				if(obj.containsKey("id")){
//					request.add(new UpdateRequest(index, type, id).doc(obj.toJSONString(),XContentType.JSON));
//				}else{
//					request.add(new IndexRequest(index, type).source(obj.toJSONString(),XContentType.JSON));
//				}
				request.add(new UpdateRequest(index, type, id).upsert(obj.toJSONString(),XContentType.JSON));
			}
			BulkResponse result = xclient.bulk(request);
			return result.toString();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String bulkDelete(String index,String type,String... ids){
		try {
			if(xclient==null){
				init();
			}
			BulkRequest request = new BulkRequest();
			for (String id : ids) {
				request.add(new DeleteRequest(index, type, id));
			}
			BulkResponse result = xclient.bulk(request);
			return result.toString();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public String select(String index,String type,String id){
		try {
			if(xclient==null){
				init();
			}
			GetRequest request = new GetRequest(index, type, id);
			GetResponse result = xclient.get(request);
			return result.getSourceAsString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String selectAll(String indexs,String types,String condition){
		try {
			if(StringUtil.isEmpty(indexs))indexs="_all";
			if(xclient==null){
				init();
			}
			SearchSourceBuilder search = new SearchSourceBuilder();
			search.query(QueryBuilders.queryStringQuery(condition)); 
			search.explain(false);
			SearchRequest request = new SearchRequest();
			request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.source(search);
			request.indices(indexs.split(","));
			request.types(types.split(","));
			SearchResponse response = xclient.search(request);
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public String selectMatchAll(String indexs,String types,String field,String value){
		try {
			if(StringUtil.isEmpty(indexs))indexs="_all";
			if(xclient==null){
				init();
			}
			SearchSourceBuilder search = new SearchSourceBuilder();
			if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)&&!(field.matches(regex)||field.matches(value))){
				search.query(QueryBuilders.matchQuery(field, value));
			}
			search.aggregation(AggregationBuilders.terms("data").field(field+".keyword"));
			search.explain(false);
			SearchRequest request = new SearchRequest();
			request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.source(search);
			request.indices(indexs.split(","));
			request.types(types.split(","));
			SearchResponse response = xclient.search(request);
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String selectMatchAll(String indexs,String types,Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges){
		try {
			if(StringUtil.isEmpty(indexs))indexs="_all";
			if(xclient==null){
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
									child.should(QueryBuilders.matchQuery(field, value));
								}
							}
							boolquery.must(child);
						}else{
							if(!value.matches(regex)){
								boolquery.must(QueryBuilders.matchQuery(field, value));
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
									child.should(QueryBuilders.matchQuery(field, value));
								}
							}
							boolquery.should(child);
						}else{
							if(!value.matches(regex)){
								boolquery.should(QueryBuilders.matchQuery(field, value));
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
									child.should(QueryBuilders.matchQuery(field, value));
								}
							}
							boolquery.mustNot(child);
						}else{
							if(!value.matches(regex)){
								boolquery.mustNot(QueryBuilders.matchQuery(field, value));
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
			SearchSourceBuilder search = new SearchSourceBuilder();
			search.query(boolquery);
			search.highlighter(highlight);
			search.explain(false);
			SearchRequest request = new SearchRequest();
			request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.source(search);
			request.indices(indexs.split(","));
			request.types(types.split(","));
			SearchResponse response = xclient.search(request);
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String selectMatchAll(String indexs, String types, Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges, String order, boolean isAsc, int pageNo,int pageSize) {
		try {
			pageNo=pageNo<1?1:pageNo;
			pageSize=pageSize<1?10:pageSize;
			if(StringUtil.isEmpty(indexs))indexs="_all";
			if(xclient==null){
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
									child.should(QueryBuilders.matchQuery(field, value));
								}
							}
							boolquery.must(child);
						}else{
							if(!value.matches(regex)){
								boolquery.must(QueryBuilders.matchQuery(field, value));
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
									child.should(QueryBuilders.matchQuery(field, value));
								}
							}
							boolquery.should(child);
						}else{
							if(!value.matches(regex)){
								boolquery.should(QueryBuilders.matchQuery(field, value));
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
									child.should(QueryBuilders.matchQuery(field, value));
								}
							}
							boolquery.mustNot(child);
						}else{
							if(!value.matches(regex)){
								boolquery.mustNot(QueryBuilders.matchQuery(field, value));
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
			SearchSourceBuilder search = new SearchSourceBuilder();
			search.query(boolquery);
			search.highlighter(highlight);
			search.from((pageNo-1)*pageSize);
			search.size(pageSize);
			search.sort(SortBuilders.scoreSort());
			if(!StringUtil.isEmpty(order)){
				search.sort(SortBuilders.fieldSort(order).order(isAsc?SortOrder.ASC:SortOrder.DESC));
			}
			search.explain(false);
			SearchRequest request = new SearchRequest();
			request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.source(search);
			request.indices(indexs.split(","));
			request.types(types.split(","));
			SearchResponse response = xclient.search(request);
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}