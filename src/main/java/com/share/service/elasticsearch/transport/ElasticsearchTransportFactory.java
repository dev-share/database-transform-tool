package com.share.service.elasticsearch.transport;

import java.net.InetAddress;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.share.service.elasticsearch.AbstractElasticsearchFactory;
import com.share.util.StringUtil;

public class ElasticsearchTransportFactory extends AbstractElasticsearchFactory{
	private static Logger logger = LogManager.getLogger();
	protected PreBuiltTransportClient client=null;
	
	private static int DEFAULT_PORT = 9300;
	
	public int defaultPort() {
		return DEFAULT_PORT;
	}
	public ElasticsearchTransportFactory() {
		super();
	}
	public ElasticsearchTransportFactory(String servers) {
		super(servers);
	}
	public ElasticsearchTransportFactory(String servers,int port) {
		super(servers, port);
	}
	public ElasticsearchTransportFactory(String clusterName, String servers,int port) {
		super(clusterName, servers, port);
	}
	public ElasticsearchTransportFactory(String clusterName, String servers, String username, String password) {
		super(clusterName, servers, username, password);
	}
	public ElasticsearchTransportFactory(String clusterName, String servers, String username, String password,int port) {
		super(clusterName, servers, username, password, port);
	}
	/**
	 * 描述: Elasticsearch服务初始化
	 * 时间: 2017年11月14日 上午10:55:02
	 * @author yi.zhang
	 */
	public void init(){
		try {
			Builder builder = Settings.builder();
			builder.put("cluster.name", clusterName);
			builder.put("client.transport.sniff", true);
			if(!StringUtil.isEmpty(username)&&!StringUtil.isEmpty(password)){
				builder.put("xpack.security.user",username+":"+password);
				Settings settings = builder.build();
				client = new PreBuiltXPackTransportClient(settings);
//				String token = UsernamePasswordToken.basicAuthHeaderValue(username, new SecureString(password.toCharArray()));
//				client.filterWithHeader(Collections.singletonMap("Authorization", token));
			}else{
				Settings settings = builder.build();
				client = new PreBuiltTransportClient(settings);
			}
			for(String server : servers.split(",")){
				String[] address = server.split(":");
				String ip = address[0];
				int _port=port;
				if(address.length>1){
					_port = Integer.valueOf(address[1]);
				}
				client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), _port));
			}
		} catch (Exception e) {
			logger.error("-----Elasticsearch Config init Error-----", e);
		}
	}
	public void close(){
		if(client!=null)client.close();
	}
	
	public PreBuiltTransportClient getClient(){
		return client;
	}
	
	public String mapping(String index, String type, @SuppressWarnings("rawtypes") Class clazz) {
		boolean isCustom = true;
		IndicesExistsResponse response = client.admin().indices().prepareExists(index).get();
		if(response!=null&&!response.isExists()){
			String body="{properties:"+JSON.toJSONString(reflect(clazz, isCustom))+"}";
			CreateIndexRequestBuilder builder = client.admin().indices().prepareCreate(index);
			builder.addMapping(type, JSON.parseObject(body).toJSONString(),XContentType.JSON);
			builder.setSource(JSON.parseObject("{settings:"+analyzer(index)+"}").toJSONString(), XContentType.JSON);
			PutMappingResponse _response = client.admin().indices().preparePutMapping(index).setType(type).setSource(JSON.parseObject(body).toJSONString(),XContentType.JSON).get();
			return _response.toString();
		}else{
			GetSettingsResponse sresponse = client.admin().indices().prepareGetSettings(index).get();
			if(sresponse!=null&&!sresponse.getIndexToSettings().isEmpty()){
				for(ObjectCursor<Settings> settings:sresponse.getIndexToSettings().values()){
					Settings setting = settings.value;
					if(setting!=null&&setting.getByPrefix("index.analysis.analyzer.es_analyzer").isEmpty()){
						isCustom = false;
					}
				}
			}
			TypesExistsResponse tresponse = client.admin().indices().prepareTypesExists(index).setTypes(type).get();
			if(tresponse!=null&&!tresponse.isExists()){
				String body="{properties:"+JSON.toJSONString(reflect(clazz, isCustom))+"}";
				PutMappingRequestBuilder builder = client.admin().indices().preparePutMapping(index);
				builder.setType(type);
				builder.setSource(JSON.parseObject(body).toJSONString(), XContentType.JSON);
				PutMappingResponse _response = builder.get();
				return _response.toString();
			}
		}
		return null;
	}
	
	public String insert(String index,String type,Object json){
		try {
			if(client==null){
				init();
			}
			IndexResponse response = client.prepareIndex(index, type).setSource(JSON.parseObject(JSON.toJSONString(json)),XContentType.JSON).execute().actionGet();
			if(response.getResult().equals(Result.CREATED)){
				System.out.println(JSON.toJSONString(response));
			}
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String update(String index,String type,String id,Object json){
		try {
			if(client==null){
				init();
			}
			UpdateResponse result = client.prepareUpdate(index, type, id).setDoc(JSON.parseObject(JSON.toJSONString(json)),XContentType.JSON).execute().actionGet();
			System.out.println(JSON.toJSONString(result));
			return result.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String upsert(String index,String type,String id,Object json){
		try {
			if(client==null){
				init();
			}
			IndexRequest indexRequest = new IndexRequest(index, type, id).source(JSON.parseObject(JSON.toJSONString(json)),XContentType.JSON);
			UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(JSON.parseObject(JSON.toJSONString(json)),XContentType.JSON).upsert(indexRequest);              
			UpdateResponse result = client.update(updateRequest).get();
			return result.toString();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String delete(String index,String type,String id){
		try {
			if(client==null){
				init();
			}
			DeleteResponse result = client.prepareDelete(index, type, id).execute().actionGet();
			System.out.println(JSON.toJSONString(result));
			return result.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String bulkUpsert(String index,String type,List<Object> jsons){
		try {
			if(client==null){
				init();
			}
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			for (Object json : jsons) {
				JSONObject obj = JSON.parseObject(JSON.toJSONString(json));
				String id = UUIDs.base64UUID();
				if(obj.containsKey("id")){
					id = obj.getString("id");
					obj.remove("id");
					bulkRequest.add(client.prepareUpdate(index, type, id).setDoc(obj.toJSONString(),XContentType.JSON));
				}else{
					bulkRequest.add(client.prepareIndex(index, type, id).setSource(obj.toJSONString(),XContentType.JSON));
				}
			}
			BulkResponse result = bulkRequest.execute().get();
			return result.toString();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	@Override
	public String bulkDelete(String index, String type, String... ids) {
		try {
			if(client==null){
				init();
			}
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			for (String id : ids) {
				bulkRequest.add(client.prepareDelete(index, type, id));
			}
			BulkResponse result = bulkRequest.execute().get();
			return result.toString();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public String drop(String indexs) {
		// TODO Auto-generated method stub
		return null;
	}
	public String select(String index,String type,String id){
		try {
			if(client==null){
				init();
			}
			GetResponse result = client.prepareGet(index, type, id).execute().actionGet();
			return result.getSourceAsString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String selectAll(String indexs,String types,String condition){
		try {
			if(client==null){
				init();
			}
			SearchRequestBuilder request = client.prepareSearch(indexs.split(",")).setTypes(types.split(","));
			request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.setQuery(QueryBuilders.queryStringQuery(condition));
			request.setExplain(false);
			SearchResponse response = request.get();
			return response.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public String selectMatchAll(String indexs,String types,String field,String value){
		try {
			if(client==null){
				init();
			}
			SearchRequestBuilder request = client.prepareSearch(indexs.split(",")).setTypes(types.split(","));
			request.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
			request.setQuery(QueryBuilders.matchQuery(field, value));
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
	public String selectMatchAll(String indexs,String types,Map<String,Object> must,Map<String,Object> should,Map<String,Object> must_not,Map<String,List<Object>> ranges){
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
	@Override
	public String selectMatchAll(String indexs, String types, Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges, String order, boolean isAsc, int pageNo,int pageSize) {
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