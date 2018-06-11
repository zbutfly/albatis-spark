package com.hzcominfo.dataggr.uniquery.mongo;

import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.*;
import com.hzcominfo.dataggr.uniquery.mongo.MongoQuery.QueryType;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import net.butfly.albacore.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class MongoQueryVisitor extends JsonBasicVisitor<MongoQuery> {

    public MongoQueryVisitor(MongoQuery mongoQuery, JsonObject json) {
        super(mongoQuery, json);
        visit(json);
    }

    @Override
    public void visitFields(List<FieldItem> items, boolean distinct) {
        DBObject sFields = new BasicDBObject();
        if (items.stream().map(FieldItem::name).anyMatch("*"::equals)) {
            get().setFields(sFields);
            return;
        }
        // 全是普通字段
        if (allFieldItemIsSimple(items)) {
            items.forEach(item -> sFields.put(item.name(), 1));
            get().setFields(sFields);
            return;
        }

        // 如果是select count(*) from t where ?

        // 是聚合操作，找到所有的聚合函数,普通字段一定在group by 里，可以忽略
        List<Pair<String, DBObject>> groupAggItems = new ArrayList<>();
        items.stream().filter(isAggregateField).map(fieldItem -> AggItem.of(fieldItem.full(), fieldItem.alias())).forEach(item -> {
            if ("COUNT".equals(item.function().toUpperCase())) {
                groupAggItems.add(Pair.of(item.alias(), new BasicDBObject("$sum", 1)));
            } else {
                // https://docs.mongodb.com/manual/reference/operator/aggregation/avg/#grp._S_avg
                groupAggItems.add(Pair.of(item.alias(), new BasicDBObject("$" + item.function().toLowerCase(), "$" + item.field())));
            }
        });
        get().setPipelineGroupAggItem(groupAggItems);

    }

    private boolean allFieldItemIsSimple(List<FieldItem> items) {
        if (null == items) return false;
        if (items.isEmpty()) return true;
        return items.stream().map(item -> !isAggregateField.test(item)).reduce(true, (b1, b2) -> b1 && b2);
    }

    private static final Predicate<FieldItem> isAggregateField = (item) -> item.name().contains("(") && item.name().contains(")");

    @Override
    public void visitTables(List<TableItem> tables) {
        // do nothing
    }

    @Override
    public void visitConditions(JsonObject json) {
        get().setQuery(MongoConditionTransverter.of(json));
    }

    @Override
    public void visitGroupBy(List<GroupItem> groups) {
        if (groups == null || groups.isEmpty()) return;
        groups.forEach(group -> dbObject.put(group.name(), "$" + group.name()));
        get().setPipelineGroupId(dbObject);
        get().setQueryType(QueryType.AGGR);
    }

    @Override
    public void visitMultiGroupBy(List<List<GroupItem>> groupsList) {

    }

    @Override
    public void visitOrderBy(List<OrderItem> orders) {
        BasicDBList order = new BasicDBList();
        if (null != orders && !orders.isEmpty()) {
            orders.forEach(oi -> order.add(new BasicDBObject(oi.name(), oi.desc() ? -1 : 1)));
        }
        get().setSort(order);
    }

    @Override
    public void visitOffset(long offset) {
        get().setOffset((int) offset);
    }

    @Override
    public void visitLimit(long limit) {
        get().setLimit((int) limit);
    }

    @Override
    public void visitIsCount(boolean idGroup) {
    	if (idGroup) return;
    	get().setQueryType(QueryType.COUNT);
    }

	@Override
	public void visitHaving(JsonObject json) {
		// TODO Auto-generated method stub
		
	}
}
