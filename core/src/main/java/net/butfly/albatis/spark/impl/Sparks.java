package net.butfly.albatis.spark.impl;

import static net.butfly.albatis.ddl.vals.ValType.Flags.BINARY;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BOOL;
import static net.butfly.albatis.ddl.vals.ValType.Flags.BYTE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.CHAR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DATE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.DOUBLE;
import static net.butfly.albatis.ddl.vals.ValType.Flags.FLOAT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.GEO;
import static net.butfly.albatis.ddl.vals.ValType.Flags.INT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.LONG;
import static net.butfly.albatis.ddl.vals.ValType.Flags.SHORT;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STR;
import static net.butfly.albatis.ddl.vals.ValType.Flags.STRL;
import static net.butfly.albatis.ddl.vals.ValType.Flags.UNKNOWN;
import static net.butfly.albatis.ddl.vals.ValType.Flags.VOID;
import static net.butfly.albatis.spark.impl.Schemas.ROW_KEY_VALUE_FIELD;
import static net.butfly.albatis.spark.impl.Schemas.ROW_TABLE_NAME_FIELD;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.vals.ValType;
import scala.collection.Seq;

public interface Sparks {
	static final Logger logger = Logger.getLogger(Sparks.class);

	@SuppressWarnings("deprecation")
	static DataType fieldType(ValType t) {
		if (null == t) return DataTypes.StringType;
		switch (t.flag) {
		case VOID:
			return DataTypes.NullType;
		case UNKNOWN:
			return DataTypes.StringType;
		// basic type: primitive
		case BOOL:
			return DataTypes.BooleanType;
		case CHAR:
			return DataTypes.StringType;
		case BYTE:
			return DataTypes.ByteType;
		case SHORT:
			return DataTypes.ShortType;
		case INT:
			return DataTypes.IntegerType;
		case LONG:
			return DataTypes.LongType;
		case FLOAT:
			return DataTypes.FloatType;
		case DOUBLE:
			return DataTypes.DoubleType;
		// basic type: extended
		case STR:
			return DataTypes.StringType;
		case STRL:
			return DataTypes.StringType;
		case BINARY:
			return DataTypes.BinaryType;
		case DATE:
			return DataTypes.TimestampType;
		// assembly type
		case GEO:
			return DataTypes.StringType;
		default:
			Logger.getLogger(SparkIO.class).warn(t.toString() + " not support for spark sql data type");
			return DataTypes.StringType;
		}
	}

	@SuppressWarnings("deprecation")
	static final Map<String, ValType> DATA_VAL_TYPE_MAPPING = Maps.of(//
			DataTypes.NullType.typeName(), ValType.VOID, //
			DataTypes.StringType.typeName(), ValType.STR, //
			// basic type: primitive
			DataTypes.BooleanType.typeName(), ValType.BOOL, //
			DataTypes.ByteType.typeName(), ValType.BYTE, //
			DataTypes.ShortType.typeName(), ValType.SHORT, //
			DataTypes.IntegerType.typeName(), ValType.INT, //
			DataTypes.LongType.typeName(), ValType.LONG, //
			DataTypes.FloatType.typeName(), ValType.FLOAT, //
			DataTypes.DoubleType.typeName(), ValType.DOUBLE, //
			// basic type: extended
			DataTypes.BinaryType.typeName(), ValType.BIN, //
			DataTypes.TimestampType.typeName(), ValType.DATE//
	);

	@SuppressWarnings("deprecation")
	static ValType valType(DataType t) {
		if (null == t) return ValType.UNKNOWN;
		ValType vt = DATA_VAL_TYPE_MAPPING.get(t.typeName());
		if (null != vt) return vt;
		Logger.getLogger(SparkIO.class).warn(t.toString() + " not support for spark sql data type");
		return ValType.STR;
	}

	static DataType classType(Class<?> c) {
		if (CharSequence.class.isAssignableFrom(c)) return DataTypes.StringType;
		if (int.class.isAssignableFrom(c) || Integer.class.isAssignableFrom(c)) return DataTypes.IntegerType;
		if (long.class.isAssignableFrom(c) || Long.class.isAssignableFrom(c)) return DataTypes.LongType;
		if (boolean.class.isAssignableFrom(c) || Boolean.class.isAssignableFrom(c)) return DataTypes.BooleanType;
		if (double.class.isAssignableFrom(c) || Double.class.isAssignableFrom(c)) return DataTypes.DoubleType;
		if (float.class.isAssignableFrom(c) || Float.class.isAssignableFrom(c)) return DataTypes.FloatType;
		if (byte.class.isAssignableFrom(c) || Byte.class.isAssignableFrom(c)) return DataTypes.ByteType;
		if (short.class.isAssignableFrom(c) || Short.class.isAssignableFrom(c)) return DataTypes.ShortType;
		if (byte[].class.isAssignableFrom(c)) return DataTypes.BinaryType;
		if (Date.class.isAssignableFrom(c)) return DataTypes.DateType;
		if (Timestamp.class.isAssignableFrom(c)) return DataTypes.TimestampType;
		if (Void.class.isAssignableFrom(c)) return DataTypes.NullType;
		// if (CharSequence.class.isAssignableFrom(c)) return
		// DataTypes.CalendarIntervalType;
		if (c.isArray()) return DataTypes.createArrayType(classType(c.getComponentType()));
		// if (Iterable.class.isAssignableFrom(c)) return
		// DataTypes.createArrayType(elementType);
		// if (Map.class.isAssignableFrom(c)) return DataTypes.createMapType(keyType,
		// valueType);
		throw new UnsupportedOperationException(c.getName() + " not support for spark sql data type");
	}

	static <T> scala.collection.Map<String, T> mapizeScala(java.util.Map<String, T> javaMap) {
		return scala.collection.JavaConversions.mapAsScalaMap(javaMap);
	}

	static <T> java.util.Map<String, T> mapizeJava(scala.collection.Map<String, T> scalaMap) {
		return scala.collection.JavaConversions.mapAsJavaMap(scalaMap);
	}

	static <T> java.util.List<T> listJava(scala.collection.Seq<T> scalaSeq) {
		return scala.collection.JavaConversions.seqAsJavaList(scalaSeq);
	}

	static <T> Seq<T> listScala(List<T> javaList) {
		return scala.collection.JavaConversions.asScalaBuffer(javaList);
	}

	public static Dataset<Row> union(Iterator<Dataset<Row>> ds) {
		if (!ds.hasNext()) return null;
		Dataset<Row> d = ds.next();
		while (ds.hasNext())
			d = d.union(ds.next());
		return d;
	}

	public static Dataset<Row> union(Iterable<Dataset<Row>> ds) {
		return union(ds.iterator());
	}

	public static String alias(Dataset<?> ds) {
		LogicalPlan p = ds.logicalPlan();
		return p instanceof SubqueryAlias ? ((SubqueryAlias) p).alias() : null;
	}

	static List<Dataset<Row>> byTable(Dataset<Row> ds) {
		List<String> keys = ds.groupBy(ROW_TABLE_NAME_FIELD).agg(count(lit(1)).alias("cnt"))//
				.map(r -> r.getAs(ROW_TABLE_NAME_FIELD), Encoders.STRING()).collectAsList();
		List<Dataset<Row>> r = Colls.list();
		keys = new ArrayList<>(keys);
		while (!keys.isEmpty()) {
			String t = keys.remove(0);
			Dataset<Row> tds;
			if (keys.isEmpty()) tds = ds;
			else {
				tds = ds.filter(col(ROW_TABLE_NAME_FIELD).equalTo(t));
				ds = ds.filter(col(ROW_TABLE_NAME_FIELD).notEqual(t)).persist();
			}
			// tds = tds.drop(ROW_TABLE_NAME_FIELD, ROW_KEY_FIELD_FIELD, ROW_KEY_VALUE_FIELD);
			logger.trace(() -> "Table split finished, got [" + t + "].");// and processing with [" + ds.count() + "] records.");
			r.add(tds.repartition(col(ROW_KEY_VALUE_FIELD)).alias(t));
		}
		return r;
	}
}
