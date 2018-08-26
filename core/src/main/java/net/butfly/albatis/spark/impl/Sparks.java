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

import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import net.butfly.albacore.utils.Configs;
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

	@SafeVarargs
	static <T> Seq<T> listScala(T... cols) {
		return scala.collection.JavaConversions.asScalaBuffer(Colls.list(cols));
	}

	public static <T> Dataset<T> union(Iterator<Dataset<T>> ds) {
		if (!ds.hasNext()) return null;
		Dataset<T> d = ds.next();
		while (ds.hasNext())
			d = d.union(ds.next());
		return d;
	}

	public static <T> Dataset<T> union(Iterable<Dataset<T>> ds) {
		return union(ds.iterator());
	}

	public static String alias(Dataset<?> ds) {
		LogicalPlan p = ds.logicalPlan();
		return p instanceof SubqueryAlias ? ((SubqueryAlias) p).alias() : null;
	}

	static <T> List<Dataset<T>> split(Dataset<T> ds, boolean forceFetch) {
		@SuppressWarnings("deprecation")
		int split = Integer.parseInt(Configs.gets("albatis.spark.split", "-1")), count = 1;
		if (split <= 0 && !forceFetch) return Colls.list(ds);
		long total = ds.count();
		if (split > 0 && total > split) return Colls.list(ds);
		try {
			for (long curr = total; curr > split; curr = curr / 2)
				count *= 2;
			double[] weights = new double[count];
			double w = 1.0 / count;
			for (int i = 0; i < count; i++)
				weights[i] = w;
			return Colls.list(ds.randomSplit(weights));
		} finally {
			logger.info("Dataset [size: " + total + "], split into [" + count + "].");
		}
	}
}
