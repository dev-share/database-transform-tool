package com.share.service.elasticsearch;

import java.util.List;
import java.util.Map;
/**
 * 描述: Elasticsearch接口
 * 时间: 2018年1月9日 上午11:21:01
 * @author yi.zhang
 * @since 1.0
 * JDK版本:1.8
 */
public interface ElasticsearchFactory {
	/**
	 * 描述: 保存数据
	 * 时间: 2018年1月9日 上午11:22:25
	 * @author yi.zhang
	 * @param index	索引
	 * @param type	类型
	 * @param json	数据对象(json对象或字符串)
	 * @return
	 */
	public String insert(String index,String type,Object json);
	/**
	 * 描述: 修改数据
	 * 时间: 2018年1月9日 上午11:22:25
	 * @author yi.zhang
	 * @param index	索引
	 * @param type	类型
	 * @param id	唯一标识
	 * @param json	数据对象(json对象或字符串)
	 * @return
	 */
	public String update(String index,String type,String id,Object json);
	/**
	 * 描述: 保存或修改数据
	 * 时间: 2018年1月9日 上午11:22:25
	 * @author yi.zhang
	 * @param index	索引
	 * @param type	类型
	 * @param id	唯一标识
	 * @param json	数据对象(json对象或字符串)
	 * @return
	 */
	public String upsert(String index,String type,String id,Object json);
	/**
	 * 描述: 删除数据
	 * 时间: 2018年1月9日 上午11:22:25
	 * @author yi.zhang
	 * @param index	索引
	 * @param type	类型
	 * @param id	唯一标识
	 * @return
	 */
	public String delete(String index,String type,String id);
	/**
	 * 描述: 批量插入或修改
	 * 时间: 2018年1月9日 上午11:25:47
	 * @author yi.zhang
	 * @param index	索引
	 * @param type	类型
	 * @param jsons 数据集合(json对象或字符串)
	 * @return
	 */
	public String bulkUpsert(String index,String type,List<Object> jsons);
	/**
	 * 描述: 批量删除
	 * 时间: 2018年1月9日 上午11:26:42
	 * @author yi.zhang
	 * @param index	索引
	 * @param type	类型
	 * @param ids	唯一标识集合
	 * @return
	 */
	public String bulkDelete(String index,String type,String... ids);
	/**
	 * 描述: 销毁索引
	 * 时间: 2018年1月9日 上午11:27:42
	 * @author yi.zhang
	 * @param indexs	索引(支持通配符)
	 * @return
	 */
	public String drop(String indexs);
	/**
	 * 描述: 查询详细信息
	 * 时间: 2018年1月9日 上午11:28:44
	 * @author yi.zhang
	 * @param index	索引
	 * @param type	类型
	 * @param id	唯一标识
	 * @return
	 */
	public String select(String index,String type,String id);
	/**
	 * 描述: 全文搜索匹配
	 * 时间: 2018年1月9日 上午11:29:22
	 * @author yi.zhang
	 * @param indexs	索引
	 * @param types		类型
	 * @param condition 条件字符串
	 * @return
	 */
	public String selectAll(String indexs,String types,String condition);
	/**
	 * 描述: 精确字段匹配
	 * 时间: 2018年1月9日 上午11:30:33
	 * @author yi.zhang
	 * @param indexs	索引
	 * @param types		类型
	 * @param field		字段
	 * @param value		字段值
	 * @return
	 */
	public String selectMatchAll(String indexs,String types,String field,String value);
	/**
	 * 描述: 精确条件匹配
	 * 时间: 2018年1月9日 上午11:31:46
	 * @author yi.zhang
	 * @param indexs	索引
	 * @param types		类型
	 * @param must		must条件
	 * @param should	should条件
	 * @param must_not	must_not条件
	 * @param ranges	范围条件
	 * @return
	 */
	public String selectMatchAll(String indexs,String types,Map<String,Object> must,Map<String,Object> should,Map<String,Object> must_not,Map<String,List<Object>> ranges);
	/**
	 * 描述: 【分页】精确条件【排序】匹配检索
	 * 时间: 2018年1月9日 上午11:33:14
	 * @author yi.zhang
	 * @param indexs	索引
	 * @param types		类型
	 * @param must		must条件
	 * @param should	should条件
	 * @param must_not	must_not条件
	 * @param ranges	范围条件
	 * @param order		排序字段
	 * @param isAsc		排序类型(true:升序,false:降序)
	 * @param pageNo	分页编号
	 * @param pageSize	分页数量
	 * @return
	 */
	public String selectMatchAll(String indexs,String types,Map<String,Object> must,Map<String,Object> should,Map<String,Object> must_not,Map<String,List<Object>> ranges,String order,boolean isAsc,int pageNo,int pageSize);
}
