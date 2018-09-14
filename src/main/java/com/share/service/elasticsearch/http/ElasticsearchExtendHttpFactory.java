package com.share.service.elasticsearch.http;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.share.util.HttpUtil;
import com.share.util.StringUtil;

public class ElasticsearchExtendHttpFactory extends ElasticsearchHttpFactory{
	public ElasticsearchExtendHttpFactory() {
		super();
	}
	public ElasticsearchExtendHttpFactory(String servers) {
		super(servers);
	}
	public ElasticsearchExtendHttpFactory(String servers,int port) {
		super(servers, port);
	}
	public ElasticsearchExtendHttpFactory(String clusterName, String servers,int port) {
		super(clusterName, servers, port);
	}
	public ElasticsearchExtendHttpFactory(String clusterName, String servers, String username, String password) {
		super(clusterName, servers, username, password);
	}
	public ElasticsearchExtendHttpFactory(String clusterName, String servers, String username, String password,int port) {
		super(clusterName, servers, username, password, port);
	}
	public String selectTermAll(String indexs, String types, String field, String value) {
		if(StringUtil.isEmpty(indexs))indexs="_all";
		String uri = "/"+indexs+(StringUtil.isEmpty(types)?"":"/"+types)+"/_search?pretty";
		String body = "";
		if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)&&!(field.matches(regex)||field.matches(value))){
			String query = "{query:{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+value+"'}}}";
			body = JSON.parseObject(query).toJSONString();
		}
		String result = base(uri, HttpUtil.METHOD_POST, body);
		return result;
	}

	public String selectTermAll(String indexs, String types,Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges) {
		if(StringUtil.isEmpty(indexs))indexs="_all";
		String uri = "/"+indexs+(StringUtil.isEmpty(types)?"":"/"+types)+"/_search?pretty";
		List<JSONObject> must_terms = new ArrayList<JSONObject>();
		List<JSONObject> should_terms = new ArrayList<JSONObject>();
		List<JSONObject> must_not_terms = new ArrayList<JSONObject>();
		if(must!=null&&must.size()>0){
			for (String field : must.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_terms = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+_value+"'}}";
								child_terms.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{must:"+JSON.toJSONString(child_terms)+"}}";
						must_terms.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+value+"'}}";
							must_terms.add(JSON.parseObject(match));
						}
					}
				}
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
						List<JSONObject> child_terms = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+_value+"'}}";
								child_terms.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{should:"+JSON.toJSONString(child_terms)+"}}";
						must_terms.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+value+"'}}";
							should_terms.add(JSON.parseObject(match));
						}
					}
				}
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
						List<JSONObject> child_terms = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+_value+"'}}";
								child_terms.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{must_not:"+JSON.toJSONString(child_terms)+"}}";
						must_not_terms.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+value+"'}}";
							must_not_terms.add(JSON.parseObject(match));
						}
					}
				}
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
					String range = "{range:{"+key+":{gte:"+start+",lt:"+end+"}}}";
					must_terms.add(JSON.parseObject(range));
				}
			};
		}
		String query = "{query:{bool:{must:"+JSON.toJSONString(must_terms)+",must_not:"+JSON.toJSONString(must_not_terms)+",should:"+JSON.toJSONString(should_terms)+"}}}";
		String body = JSON.parseObject(query).toJSONString();
		String result = base(uri, HttpUtil.METHOD_POST, body);
		return result;
	}
	public String selectTermAll(String indexs, String types, Map<String, Object> must, Map<String, Object> should, Map<String, Object> must_not, Map<String, List<Object>> ranges, String order, boolean isAsc, int pageNo, int pageSize) {
		if(StringUtil.isEmpty(indexs))indexs="_all";
		pageNo=pageNo<1?1:pageNo;
		pageSize=pageSize<1?10:pageSize;
		String uri = "/"+indexs+(StringUtil.isEmpty(types)?"":"/"+types)+"/_search?pretty&size="+pageSize+"&from"+(pageNo-1)*pageSize;
		List<JSONObject> must_terms = new ArrayList<JSONObject>();
		List<JSONObject> should_terms = new ArrayList<JSONObject>();
		List<JSONObject> must_not_terms = new ArrayList<JSONObject>();
		if(must!=null&&must.size()>0){
			for (String field : must.keySet()) {
				if(field.matches(regex)){
					continue;
				}
				Object text = must.get(field);
				String value = text instanceof String ?text.toString():JSON.toJSONString(text);
				if(!StringUtil.isEmpty(field)&&!StringUtil.isEmpty(value)){
					if(value.startsWith("[")&&value.endsWith("]")){
						List<JSONObject> child_terms = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+_value+"'}}";
								child_terms.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{must:"+JSON.toJSONString(child_terms)+"}}";
						must_terms.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+value+"'}}";
							must_terms.add(JSON.parseObject(match));
						}
					}
				}
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
						List<JSONObject> child_terms = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+_value+"'}}";
								child_terms.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{should:"+JSON.toJSONString(child_terms)+"}}";
						must_terms.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+value+"'}}";
							should_terms.add(JSON.parseObject(match));
						}
					}
				}
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
						List<JSONObject> child_terms = new ArrayList<JSONObject>();
						List<String> values = JSON.parseArray(value, String.class);
						for (String _value : values) {
							if(!_value.matches(regex)){
								String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+_value+"'}}";
								child_terms.add(JSON.parseObject(match));
							}
						}
						String match = "{bool:{must_not:"+JSON.toJSONString(child_terms)+"}}";
						must_not_terms.add(JSON.parseObject(match));
					}else{
						if(!value.matches(regex)){
							String match = "{term:{"+(field.endsWith(".keyword")?field:field+".keyword")+":'"+value+"'}}";
							must_not_terms.add(JSON.parseObject(match));
						}
					}
				}
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
					String range = "{range:{"+key+":{gte:"+start+",lt:"+end+"}}}";
					must_terms.add(JSON.parseObject(range));
				}
			};
		}
		List<JSONObject> sorts = new ArrayList<JSONObject>();
		sorts.add(JSON.parseObject("{_score:{order:'desc'}}"));
		if(!StringUtil.isEmpty(order)){
			sorts.add(JSON.parseObject("{"+order+":{order:'"+(isAsc?"asc":"desc")+"'}}"));
		}
		String query = "{query:{bool:{must:"+JSON.toJSONString(must_terms)+",must_not:"+JSON.toJSONString(must_not_terms)+",should:"+JSON.toJSONString(should_terms)+"}},sort:"+JSON.toJSONString(sorts)+"}";
		String body = JSON.parseObject(query).toJSONString();
		String result = base(uri, HttpUtil.METHOD_POST, body);
		return result;
	}
}
