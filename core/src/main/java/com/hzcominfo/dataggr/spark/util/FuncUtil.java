
package com.hzcominfo.dataggr.spark.util;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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
import net.butfly.albatis.io.R;
import scala.Function0;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;

public class FuncUtil implements Serializable {
	private static final long serialVersionUID = -8305619702897096234L;

	@SuppressWarnings("rawtypes")
	public static final Encoder<Map> ENC_MAP = Encoders.javaSerialization(Map.class);
	public static final Encoder<R> ENC_R = Encoders.javaSerialization(R.class);

	public static Function<URISpec, String> defaultcoll = u -> {
		String file = u.getFile();
		String[] path = u.getPaths();
		String tbl = null;
		if (path.length > 0) tbl = file;
		return tbl;
	};

	public static Row mapRow(java.util.Map<String, Object> map) {
		List<StructField> fields = Colls.list();
		map.forEach((k, v) -> fields.add(DataTypes.createStructField(k, classType(v), null == v)));
		return new GenericRowWithSchema(map.values().toArray(), DataTypes.createStructType(fields));
	}

	public static Row mapRow(R map) {
		List<StructField> fields = Colls.list();
		map.put("___table", map.table());
		map.forEach((k, v) -> fields.add(DataTypes.createStructField(k, classType(v), null == v)));
		return new GenericRowWithSchema(map.values().toArray(), DataTypes.createStructType(fields));
	}

	public static Map<String, Object> rowMap(Row row) {
		Seq<String> seq = JavaConverters.asScalaIteratorConverter(Arrays.asList(row.schema().fieldNames()).iterator()).asScala().toSeq();
		Map<String, Object> map = JavaConversions.mapAsJavaMap(row.getValuesMap(seq));
		String t = (String) map.remove("___table");
		if (null != t) return new R(t, map);
		else return map;
	}

	public static R rMap(Row row) {
		Seq<String> seq = JavaConverters.asScalaIteratorConverter(Arrays.asList(row.schema().fieldNames()).iterator()).asScala().toSeq();
		Map<String, Object> map = JavaConversions.mapAsJavaMap(row.getValuesMap(seq));
		String t = (String) map.remove("___table");
		if (null != t) return new R(t, map);
		else return new R(map);
	}

	public static final DataType classType(Object v) {
		return classType(null == v ? Void.class : v.getClass());
	}

	public static final DataType classType(Class<?> c) {
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

	public static <T> Seq<T> dataset(Iterable<T> rows) {
		return JavaConverters.asScalaIteratorConverter(rows.iterator()).asScala().toSeq();
	}

	public static <RR> Function0<RR> func0(Supplier<RR> f) {
		return new AbstractFunction0<RR>() {
			@Override
			public RR apply() {
				return f.get();
			}
		};
	}

	public static scala.collection.Map<String, Object> mapizeScala(java.util.Map<String, Object> value) {
		return scala.collection.JavaConversions.mapAsScalaMap(value);
	}

	public static java.util.Map<String, Object> mapizeJava(scala.collection.Map<String, Object> vs) {
		return scala.collection.JavaConversions.mapAsJavaMap(vs);
	}

	// public static final Encoder<R> enc = (Encoder<R>) null;
	// public static Dataset<R> mapize(Dataset<R> ds) {
	// Encoder<R> enc = ds.sparkSession().implicits().newMapEncoder(TYPETAG_MAP);
	// return ds.map(FuncUtil::rowMap, enc);
	// }

	// public static final TypeTag<R> TYPETAG_MAP = mapenc();
	//
	// private static TypeTag<R> mapenc() {
	// JavaUniverse ru = scala.reflect.runtime.package$.MODULE$.universe();
	// Universe u = (Universe) ru;
	// JavaMirror rm = ru.runtimeMirror(SparkIO.class.getClassLoader());
	// @SuppressWarnings("rawtypes")
	// Mirror m = (Mirror) rm;
	// TypeApi t = u.appliedType(rm.classSymbol(Map.class).toType(), //
	// JavaConverters.asScalaIteratorConverter(Arrays.asList(//
	// rm.classSymbol(String.class).toType(), //
	// rm.classSymbol(Object.class).toType()//
	// ).iterator()).asScala().toSeq());
	// @SuppressWarnings("unchecked")
	// TypeTags.TypeTag<R> ct = ((Universe) ru).TypeTag().apply(m, new TypeCr(t));
	// // new PredefTypeTag(u, t, null);
	// return ct;
	// }
	//
	// private static class TypeCr extends TypeCreator {
	// private static final long serialVersionUID = 2032299227806319007L;
	// private final TypeApi t;
	//
	// public TypeCr(TypeApi t) {
	// super();
	// this.t = t;
	// }
	//
	// @Override
	// public TypeApi apply(Mirror arg0) {
	// return t;
	// }
	// }
}
