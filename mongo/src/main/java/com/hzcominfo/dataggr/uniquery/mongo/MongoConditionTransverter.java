package com.hzcominfo.dataggr.uniquery.mongo;

import static com.hzcominfo.dataggr.uniquery.ConditionTransverter.valueOf;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.hzcominfo.dataggr.uniquery.ConditionTransverter;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

public interface MongoConditionTransverter extends ConditionTransverter {
    
    DBObject toMongoQuery();

    static DBObject of(JsonObject json) {
        if (null == json || 0 == json.size()) return new UnrecognizedMongoConditionTransverter().toMongoQuery();
        JsonElement element;
        JsonObject object, jl, jr;
        JsonArray array;
        String field;
        List<Double> params;
        for (String key : json.keySet()) {
            switch (key) {
                case "and":
                case "or":
                    array = json.getAsJsonArray(key);
                    jl = array.get(0).getAsJsonObject();
                    jr = array.get(1).getAsJsonObject();
                    return "and".equals(key) ? new AndMongoConditionTransverter(of(jl), of(jr)).toMongoQuery() : new OrMongoConditionTransverter(of(jl), of(jr)).toMongoQuery();
                case "is_null":
                case "is_not_null":
                    field = json.get(key).getAsString();
                    return "is_null".equals(key) ? new IsNullMongoConditionTransverter(field).toMongoQuery() : new IsNotNullMongoConditionTransverter(field).toMongoQuery();
                case "equals":
                case "not_equals":
                case "greater_than":
                case "greater_than_or_equal":
                case "less_than":
                case "less_than_or_equal":
                case "like":
                case "not_like":
                    object = json.getAsJsonObject(key);
                    field = new ArrayList<>(object.keySet()).get(0);
                    element = object.get(field);
                    Object value = valueOf(element); // 只可能是基本类型
                    switch (key) {
                        case "equals":
                            return new EqualsMongoConditionTransverter(field, value).toMongoQuery();
                        case "not_equals":
                            return new NotEqualsMongoConditionTransverter(field, value).toMongoQuery();
                        case "greater_than":
                            return new GtMongoConditionTransverter(field, value).toMongoQuery();
                        case "greater_than_or_equal":
                            return new GeMongoConditionTransverter(field, value).toMongoQuery();
                        case "less_than":
                            return new LtMongoConditionTransverter(field, value).toMongoQuery();
                        case "less_than_or_equal":
                            return new LeMongoConditionTransverter(field, value).toMongoQuery();
                        case "like":
                            return new LikeMongoConditionTransverter(field, value).toMongoQuery();
                        case "not_like":
                            return new NotLikeMongoConditionTransverter(field, value).toMongoQuery();
                    }
                case "between":
                case "not_between":
                    object = json.getAsJsonObject(key);
                    field = new ArrayList<>(object.keySet()).get(0);
                    List<Object> se = new ArrayList<>();
                    object.get(field).getAsJsonArray().forEach(e -> se.add(e.getAsString()));
                    return "between".equals(key) ? new BetweenMongoConditionTransverter(field, se.get(0), se.get(1)).toMongoQuery()
                            : new NotBetweenMongoConditionTransverter(field, se.get(0), se.get(1)).toMongoQuery();
                case "in":
                case "not_in":
                    object = json.getAsJsonObject(key);
                    field = new ArrayList<>(object.keySet()).get(0);
                    List<Object> values = new ArrayList<>();
                    object.get(field).getAsJsonArray().forEach(e -> values.add(e.getAsString()));
                    return "in".equals(key) ? new InMongoConditionTransverter(field, values).toMongoQuery()
                            : new NotInMongoConditionTransverter(field, values).toMongoQuery();
                case "geo_distance":
    				object = json.getAsJsonObject(key);
    				field = new ArrayList<>(object.keySet()).get(0);
    				params = new ArrayList<>();
    				object.get(field).getAsJsonArray().forEach(e -> params.add(e.getAsBigDecimal().doubleValue()));
    				return new GeoDistanceMongoConditionTransverter(field, params).toMongoQuery();
    			case "geo_box":
    				object = json.getAsJsonObject(key);
    				field = new ArrayList<>(object.keySet()).get(0);
    				params = new ArrayList<>();
    				object.get(field).getAsJsonArray().forEach(e -> params.add(e.getAsBigDecimal().doubleValue()));
    				return new GeoBoxMongoConditionTransverter(field, params).toMongoQuery();
    			case "geo_polygon":
    				object = json.getAsJsonObject(key);
    				field = new ArrayList<>(object.keySet()).get(0);
    				params = new ArrayList<>();
    				object.get(field).getAsJsonArray().forEach(e -> params.add(e.getAsBigDecimal().doubleValue()));
    				return new GeoPolygonMongoConditionTransverter(field, params).toMongoQuery();
            }
        }
        throw new RuntimeException("Can NOT parse " + json + "to Mongo Query");
    }
    
 // geo_distance(field,x,y,d)
 	class GeoDistanceMongoConditionTransverter implements MongoConditionTransverter {
 		private String field;
 		private List<Double> params;

 		public GeoDistanceMongoConditionTransverter(String field, List<Double> params) {
 			this.field = field;
 			this.params = params;
 			if (params.size() != 3)
 				throw new RuntimeException("Incorrect params of GeoDistance");
 		}

		@Override
		public DBObject toMongoQuery() {
			// 坐标系单位 m ？？
//			return QueryBuilder.start(field).withinCenter(params.get(0), params.get(1), params.get(2) * 1000).get();
			return QueryBuilder.start(field).withinCenter(params.get(1), params.get(0), params.get(2)).get();
		}
 	}

 	// geo_box(field,top,left,bottom,right)
 	class GeoBoxMongoConditionTransverter implements MongoConditionTransverter {
 		private String field;
 		private List<Double> params;

 		public GeoBoxMongoConditionTransverter(String field, List<Double> params) {
 			this.field = field;
 			this.params = params;
 			if (params.size() != 4)
 				throw new RuntimeException("Incorrect params of GeoBox");
 		}

		@Override
		public DBObject toMongoQuery() {
			return QueryBuilder.start(field).withinBox(params.get(1), params.get(0), params.get(3), params.get(2)).get();
		}

 	}

 	// geo_polygon(field,x1,y1,x2,y2...xn,yn,x1,y1)
 	class GeoPolygonMongoConditionTransverter implements MongoConditionTransverter {
 		private String field;
 		private List<Double> params;

 		public GeoPolygonMongoConditionTransverter(String field, List<Double> params) {
 			this.field = field;
 			this.params = params;
 			if (params.size() < 6 || isOdd(params.size()))
 				throw new RuntimeException("Incorrect params of GeoPolygon");
 		}

		@Override
		public DBObject toMongoQuery() {
			List<Double[]> points = new ArrayList<>();
			for (int i = 0; i < params.size(); i++)
				if (!isOdd(i))
					points.add(new Double[]{params.get(i + 1), params.get(i)});
			return QueryBuilder.start(field).withinPolygon(points).get();
		}
		
		private boolean isOdd(int a) {
 			if ((a & 1) == 1) {
 				return true;
 			}
 			return false;
 		}
 	}
    
    class UnrecognizedMongoConditionTransverter implements MongoConditionTransverter {

        @Override
        public DBObject toMongoQuery() {
            return QueryBuilder.start().get();
        }
    }
    
    class AndMongoConditionTransverter implements MongoConditionTransverter {
        private DBObject left, right;

        AndMongoConditionTransverter(DBObject left, DBObject right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public DBObject toMongoQuery() {
            return QueryBuilder.start().and(left, right).get();
        }
    }

    class OrMongoConditionTransverter implements MongoConditionTransverter {
        private DBObject left, right;

        OrMongoConditionTransverter(DBObject left, DBObject right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public DBObject toMongoQuery() {
            return QueryBuilder.start().or(left, right).get();
        }
    }

    class EqualsMongoConditionTransverter implements MongoConditionTransverter {
        private String field;
        private Object value;

        EqualsMongoConditionTransverter(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public DBObject toMongoQuery() {
            return QueryBuilder.start(field).is(value).get();
        }
    }

    class NotEqualsMongoConditionTransverter implements MongoConditionTransverter {
        private String field;
        private Object value;

        NotEqualsMongoConditionTransverter(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public DBObject toMongoQuery() {
            return QueryBuilder.start(field).notEquals(value).get();
        }
    }

    class LtMongoConditionTransverter implements MongoConditionTransverter {
        private String field;
        private Object value;

        LtMongoConditionTransverter(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public DBObject toMongoQuery() {
            return QueryBuilder.start(field).lessThan(value).get();
        }
    }

    class LeMongoConditionTransverter implements MongoConditionTransverter {
        private String field;
        private Object value;

        LeMongoConditionTransverter(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public DBObject toMongoQuery() {
            return QueryBuilder.start(field).lessThanEquals(value).get();
        }
    }

    class GtMongoConditionTransverter implements MongoConditionTransverter {
        private String field;
        private Object value;

        GtMongoConditionTransverter(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public DBObject toMongoQuery() {
            return QueryBuilder.start(field).greaterThan(value).get();
        }
    }

    class GeMongoConditionTransverter implements MongoConditionTransverter {
        private String field;
        private Object value;

        GeMongoConditionTransverter(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public DBObject toMongoQuery() {
            return QueryBuilder.start(field).greaterThanEquals(value).get();
        }
    }

    class IsNullMongoConditionTransverter implements MongoConditionTransverter {
        private String field;

        IsNullMongoConditionTransverter(String field) {
            this.field = field;
        }

        @Override
        public DBObject toMongoQuery() {
            return QueryBuilder.start(field).exists(false).get();
        }
    }

    class IsNotNullMongoConditionTransverter implements MongoConditionTransverter {
        private String field;

        IsNotNullMongoConditionTransverter(String field) {
            this.field = field;
        }

        @Override
        public DBObject toMongoQuery() {
            return QueryBuilder.start(field).exists(true).get();
        }
    }

    class LikeMongoConditionTransverter implements MongoConditionTransverter {
        private String field;
        private String value;

        LikeMongoConditionTransverter(String field, Object value) {
            this.field = field;
            if (!String.class.isInstance(value)) throw new RuntimeException(value + " is not a String");
            this.value = (String) value;
        }

        @Override
        public DBObject toMongoQuery() {
            if (!value.startsWith("%")) value = "^" + value;
            if (!value.endsWith("%")) value = value + "$";
            value = value.replaceAll("_", ".").replaceAll("%", ".*");
            return QueryBuilder.start(field).regex(Pattern.compile(value, Pattern.CASE_INSENSITIVE)).get();
        }
    }

    class NotLikeMongoConditionTransverter implements MongoConditionTransverter {
        private String field;
        private String value;

        NotLikeMongoConditionTransverter(String field, Object value) {
            this.field = field;
            if (!String.class.isInstance(value)) throw new RuntimeException(value + " is not a String");
            this.value = (String) value;
        }

        @Override
        public DBObject toMongoQuery() {
            if (!value.startsWith("%")) value = "^" + value;
            if (!value.endsWith("%")) value = value + "$";
            value = value.replaceAll("_", ".").replaceAll("%", ".*");
            return QueryBuilder.start(field).not().regex(Pattern.compile(value, Pattern.CASE_INSENSITIVE)).get();
        }
    }

    class BetweenMongoConditionTransverter implements MongoConditionTransverter {
        private String field;
        private Object start, end;

        BetweenMongoConditionTransverter(String field, Object start, Object end) {
            this.field =  field;
            this.start = start;
            this.end =  end;
        }

        @Override
        public DBObject toMongoQuery() {
            return QueryBuilder.start(field).greaterThanEquals(start).lessThanEquals(end).get();
        }
    }

    class NotBetweenMongoConditionTransverter implements MongoConditionTransverter {
        private String field;
        private Object start, end;

        NotBetweenMongoConditionTransverter(String field, Object start, Object end) {
            this.field =  field;
            this.start = start;
            this.end =  end;
        }

        @Override
        public DBObject toMongoQuery() {
            return QueryBuilder.start(field).not().greaterThanEquals(start).lessThanEquals(end).get();
        }
    }

    class InMongoConditionTransverter implements MongoConditionTransverter {
        private String field;
        private List<Object> values;

        InMongoConditionTransverter(String field, List<Object> values) {
            assert  values != null;
            this.field = field;
            this.values = values;
        }

        @Override
        public DBObject toMongoQuery() {
            BasicDBList list = new BasicDBList();
            list.addAll(values);
            return QueryBuilder.start(field).in(list).get();
        }
    }

    class NotInMongoConditionTransverter implements MongoConditionTransverter {
        private String field;
        private List<Object> values;

        NotInMongoConditionTransverter(String field, List<Object> values) {
            assert  values != null;
            this.field = field;
            this.values = values;
        }

        @Override
        public DBObject toMongoQuery() {
            BasicDBList list = new BasicDBList();
            list.addAll(values);
            return QueryBuilder.start(field).notIn(list).get();
        }
    }
}
