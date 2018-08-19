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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import com.google.common.base.Supplier;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.vals.ValType;
import net.butfly.albatis.io.Rmap;
import scala.Function0;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;

public interface Sparks {
	@SuppressWarnings("rawtypes")
	static final Encoder<Map> ENC_MAP = Encoders.javaSerialization(Map.class);
	static final Encoder<Rmap> ENC_R = Encoders.javaSerialization(Rmap.class);

	static String defaultColl(URISpec u) {
		String file = u.getFile();
		String[] path = u.getPaths();
		String tbl = null;
		if (path.length > 0) tbl = file;
		return tbl;
	};

	static Row mapRow(java.util.Map<String, Object> map) {
		List<StructField> fields = Colls.list();
		map.forEach((k, v) -> fields.add(DataTypes.createStructField(k, classType(v), null == v)));
		return new GenericRowWithSchema(map.values().toArray(), DataTypes.createStructType(fields));
	}

	static Map<String, Object> rowMap(Row row) {
		Seq<String> seq = JavaConverters.asScalaIteratorConverter(Arrays.asList(row.schema().fieldNames()).iterator()).asScala().toSeq();
		Map<String, Object> map = JavaConversions.mapAsJavaMap(row.getValuesMap(seq));
		String t = (String) map.remove("___table");
		if (null != t) return new Rmap(t, map);
		else return map;
	}

	static Rmap rmap(String table, Row row) {
		return new Rmap(table, Sparks.rowMap(row));
	}

	static DataType classType(Object v) {
		return classType(null == v ? Void.class : v.getClass());
	}

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

	static <T> Seq<T> dataset(Iterable<T> rows) {
		return JavaConverters.asScalaIteratorConverter(rows.iterator()).asScala().toSeq();
	}

	static <RR> Function0<RR> func0(Supplier<RR> f) {
		return new AbstractFunction0<RR>() {
			@Override
			public RR apply() {
				return f.get();
			}
		};
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

	static String debug(Row row) {
		String s = row.schema().simpleString() + "==>";
		for (int i = 0; i < row.schema().fields().length; i++)
			s += row.schema().fields()[i].name() + ":" + row.get(i);
		return s;
	}
}
