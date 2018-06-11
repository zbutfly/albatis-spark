package com.hzcominfo.dataggr.uniquery.mongo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonObject;
import com.hzcominfo.albatis.nosql.Connection;
import com.hzcominfo.dataggr.uniquery.Adapter;
import com.hzcominfo.dataggr.uniquery.dto.ResultSet;
import com.hzcominfo.dataggr.uniquery.mongo.MongoQuery.QueryType;
import com.mongodb.AggregationOptions;
import com.mongodb.Cursor;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import net.butfly.albatis.mongodb.MongoConnection;

public class MongoAdapter extends Adapter {
	final static String schema = "mongodb";
	public MongoAdapter() {}

	@SuppressWarnings("unchecked")
	@Override
	public MongoQuery queryAssemble(Connection connection, JsonObject sqlJson) {
		return new MongoQueryVisitor(new MongoQuery(), sqlJson).get();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T queryExecute(Connection connection, Object query, String table) {
		MongoQuery mq = (MongoQuery) query;
        DBCollection coll = ((MongoConnection) connection).collection(table);
    	if (mq.getQueryType() == QueryType.AGGR) {
    		List<DBObject> pipeline = mq.getPipeline();
    		Cursor cursor = coll.aggregate(pipeline, AggregationOptions.builder().allowDiskUse(true).build());
    		return (T) cursor;
    	} else if (mq.getQueryType() == QueryType.COUNT) {
    		long total = coll.count(mq.getQuery());
    		return (T) count(total);
    	} else {
    		DBCursor cursor = coll.find(mq.getQuery(), mq.getFields()).sort(mq.getSort()).skip(mq.getOffset()).limit(mq.getLimit());
    		return (T) cursor;
    	}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T resultAssemble(Object result) {
		if (result instanceof DBCursor) {
			return (T) common((DBCursor) result); 
		} else if (result instanceof Cursor) {
			return (T) aggr((Cursor) result);
		}
		return null;
	}
	
	private ResultSet common(DBCursor cursor) {
		ResultSet rs = new ResultSet();
		List<Map<String, Object>> resultMap = new ArrayList<>();
		while (cursor.hasNext()) {
			DBObject doc = cursor.next();
			Map<String, Object> map = new HashMap<>();
			for(String key : doc.keySet()) {
				map.put(key, doc.get(key));
			}
			resultMap.add(map);
		}
		rs.setTotal(resultMap.size());
		rs.setResults(resultMap);
		return rs;
	}
	
	private ResultSet aggr(Cursor cursor) {
		ResultSet rs = new ResultSet();
		List<Map<String, Object>> resultMap = new ArrayList<>();
		while (cursor.hasNext()) {
			DBObject doc = cursor.next();
			Map<String, Object> map = new HashMap<>();
			for(String key : doc.keySet()) {
				map.put(key, doc.get(key));
			}
			resultMap.add(map);
		}
		rs.setTotal(resultMap.size());
		rs.setResults(resultMap);
		return rs; 
	}
	
	private ResultSet count(long total) {
		ResultSet rs = new ResultSet();
		rs.setTotal(total);
		return rs;
	}
}
