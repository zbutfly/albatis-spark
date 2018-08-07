package com.hzcominfo.dataggr.uniquery.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import net.butfly.albacore.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public class MongoQuery {
    private DBObject query;
    private DBObject fields;
    private DBObject sort;
    private List<String> groupFields;
    private List<Pair<String, DBObject>> pipelineGroupAggItem;
    private Integer offset;
    private Integer limit;
    private List<DBObject> pipeline;
    private QueryType queryType;
    
    public static enum QueryType {
    	COMMON, AGGR, COUNT
    }

    public DBObject getQuery() {
        return query;
    }

    void setQuery(DBObject query) {
        this.query = query;
    }

    public DBObject getFields() {
        return fields;
    }

    void setFields(DBObject fields) {
        this.fields = fields;
    }

    public DBObject getSort() {
        return sort;
    }

    void setSort(DBObject sort) {
        this.sort = sort;
    }

    void addPipelineGroupFields(List<String> groups) {
        if (null == groupFields) groupFields = new ArrayList<>();
        groupFields.addAll(groups);
    }

    private DBObject getPipelineGroupId() {
        if (null == groupFields || groupFields.isEmpty()) return null;
        DBObject dbObject = new BasicDBObject();
        groupFields.forEach(group -> dbObject.put(group, "$" + group));
        return dbObject;
    }

    void setPipelineGroupAggItem(List<Pair<String, DBObject>> pipelineGroupAggItem) {
        if (null == this.pipelineGroupAggItem)
            this.pipelineGroupAggItem = pipelineGroupAggItem;
        else
            this.pipelineGroupAggItem.addAll(pipelineGroupAggItem);
    }

    public int getOffset() {
        return offset;
    }

    void setOffset(int offset) {
        this.offset = offset;
    }

    public int getLimit() {
        return limit;
    }

    void setLimit(int limit) {
        this.limit = limit;
    }

    public List<DBObject> getPipeline() {
        if (null != pipeline) return pipeline;
        pipeline = new ArrayList<>();
        pipeline.add(new BasicDBObject("$match", query));
        if (null != sort && sort.keySet().size() > 0) pipeline.add(new BasicDBObject("$sort", sort));
        if (null != offset) pipeline.add(new BasicDBObject("$skip", offset));
        if (null != limit) pipeline.add(new BasicDBObject("$limit", limit));
        BasicDBObject group = new BasicDBObject();
        group.append("_id", getPipelineGroupId());
        pipelineGroupAggItem.forEach(pair -> group.append(pair.v1(), pair.v2()));
        pipeline.add(new BasicDBObject("$group", group));
        BasicDBObject project = new BasicDBObject();
        project.append("_id", false);
        groupFields.forEach(gs -> project.append(gs, "$_id." + gs));
        pipelineGroupAggItem.forEach(pair -> project.append(pair.v1(), "$" + pair.v1()));
        pipeline.add(new BasicDBObject("$project", project));
        logger.debug("pipeline: " + pipeline);
        return pipeline;
    }

	public QueryType getQueryType() {
		return queryType;
	}

	public void setQueryType(QueryType queryType) {
		this.queryType = queryType;
	}
}
