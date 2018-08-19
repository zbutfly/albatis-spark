package net.butfly.albatis.spark.impl;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.io.IO;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.input.SparkDataInput;
import net.butfly.albatis.spark.util.DSdream;

public abstract class SparkIO implements IO, Serializable {
	private static final long serialVersionUID = 3265459356239387878L;
	private static final Map<Class<? extends IO>, Map<String, Class<? extends SparkIO>>> ADAPTERS //
			= Maps.of(Input.class, Maps.of(), Output.class, Maps.of());
	static {
		scan();
	}

	public final SparkSession spark;
	public final URISpec targetUri;
	protected final String[] tables;

	protected SparkIO(SparkSession spark, URISpec targetUri, String... table) {
		super();
		this.spark = spark;
		this.targetUri = targetUri;
		String[] t;
		if (table.length > 0) t = table;
		else if (null != targetUri && null != targetUri.getFile()) t = new String[] { targetUri.getFile() };
		else t = new String[0];
		tables = t;
	}

	public Map<String, String> options() {
		return Maps.of();
	}

	public String format() {
		return null;
	}

	public static <V, O extends Output<V>> O output(SparkSession spark, URISpec uri, String... table) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<O> cls = (Class<O>) ADAPTERS.get(Output.class).get(s);
			if (null == cls) {
				int c = s.lastIndexOf(":");
				if (c >= 0) s = s.substring(0, c);
				else break;
			} else try {
				return cls.getConstructor(SparkSession.class, URISpec.class, String[].class).newInstance(spark, uri, table);
			} catch (SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	public static <V, I extends SparkDataInput> I input(SparkSession spark, URISpec uri, String... table) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<I> cls = (Class<I>) ADAPTERS.get(Input.class).get(s);
			if (null == cls) {
				int c = s.lastIndexOf(":");
				if (c >= 0) s = s.substring(0, c);
				else break;
			} else try {
				return cls.getConstructor(SparkSession.class, URISpec.class, String[].class).newInstance(spark, uri, table);
			} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
					| IllegalArgumentException e) {
				throw new RuntimeException(e);
			} catch (InvocationTargetException e) {
				throw new RuntimeException(e.getTargetException());
			}
		}
		throw new RuntimeException("No matched adapter with scheme: " + s);
	}

	private static void scan() {
		for (Class<? extends SparkIO> cls : Reflections.getSubClasses(SparkIO.class)) {
			Schema schema = cls.getAnnotation(Schema.class);
			if (null != schema) {
				if (Input.class.isAssignableFrom(cls)) reg(Input.class, schema, cls);
				else if (Output.class.isAssignableFrom(cls)) reg(Output.class, schema, cls);
			}
		}
		Logger.getLogger(SparkIO.class).debug("Spark adaptors scanned.");
	}

	private static void reg(Class<? extends IO> io, Schema schema, Class<? extends SparkIO> cls) {
		Logger l = Logger.getLogger(cls);
		for (String s : schema.value())
			ADAPTERS.get(io).compute(s, (ss, existed) -> {
				if (null == existed) {
					l.debug("Spark[Output] schema [" + ss + "] register for class:  " + cls.getName());
					return cls;
				} else {
					Schema s0 = existed.getAnnotation(Schema.class);
					if (s0.priority() > schema.priority()) {
						l.warn("Spark[Output] schema [" + ss + "] conflicted and ingored for class:  " + cls.toString() //
								+ "\n\t(existed: " + existed.getName() + ")");
						return existed;
					} else {
						l.warn("Spark[Output] schema [" + ss + "] conflicted and ingored for class:  " + existed.toString() //
								+ "\n\t(priority: " + cls.getName() + ")");
						return cls;
					}
				}
			});
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public static @interface Schema {
		String[] value();

		int priority() default 0;
	}

	public String table() {
		// String[] tables = tables.getValue();
		if (null == tables || tables.length == 0) //
			throw new RuntimeException("No table defined for spark i/o.");
		if (tables.length > 1) Logger.getLogger(this.getClass()).warn("Multiple tables defined [" + tables
				+ "] for spark i/o, now only support the first: [" + tables[0] + "].");
		return tables[0];
	}

	@Override
	public int features() {
		return IO.super.features() | IO.Feature.SPARK;
	}

	@SuppressWarnings("unchecked")
	@Deprecated
	protected final DSdream<Rmap> dsd(Sdream<?> s) {
		return DSdream.of(spark.sqlContext(), (Sdream<Rmap>) s);
	}

	protected final <T> DSdream<Rmap> dsd(Sdream<T> s, Function<T, Rmap> conv) {
		if (s instanceof DSdream) return (DSdream<Rmap>) s.map(conv);
		List<Rmap> l = s.map(conv).list();
		Dataset<Rmap> ss = spark.sqlContext().createDataset(l, Sparks.ENC_R);
		return DSdream.of(ss);
	}

	// ==========
	private final List<FieldDesc> schema = Colls.list();

	@Override
	public List<FieldDesc> schema() {
		return schema;
	}

	public StructType schemaSpark() {
		if (schema.isEmpty()) return null;
		return new StructType(Colls.list(schema, //
				f -> new StructField(f.name, Sparks.fieldType(f.type), true, Metadata.empty())).toArray(new StructField[0]));
	}

	@Override
	public void schema(FieldDesc... field) {
		schema.clear();
		schema.addAll(Arrays.asList(field));
	}

	@Override
	public void schemaless() {
		schema.clear();
	}

	protected final static String ROW_TABLE_NAME_FIELD = "___table";
	protected final static String ROW_KEY_VALUE_FIELD = "___key_value";
	protected final static String ROW_KEY_FIELD_FIELD = "___key_field";

	protected final Rmap row2rmap(Row row) {
		Map<String, Object> m = Maps.of();
		Sparks.mapizeJava(row.getValuesMap(Sparks.listScala(Arrays.asList(row.schema().fieldNames())))).forEach((k, v) -> {
			if (null != v) m.put(k, v);
		});
		String t = (String) m.remove(ROW_TABLE_NAME_FIELD);
		String k = (String) m.remove(ROW_KEY_VALUE_FIELD);
		String kf = (String) m.remove(ROW_KEY_FIELD_FIELD);
		return new Rmap(t, k, m).keyField(kf);
	}

	protected final Row rmap2row0(Rmap r) {
		StructType s = sparkSchema();
		int l = s.fields().length;
		Object[] vs = new Object[l];
		for (int i = 0; i < l - 1; i++)
			vs[i] = r.get(s.fields()[i].name());
		return new GenericRowWithSchema(vs, s);
	}

	protected final Dataset<Row> rmap2rowDs(Dataset<Rmap> ds) {
		if (schema.isEmpty()) throw new UnsupportedOperationException("No schema io could not map back to row.");
		StructType s = sparkSchema(//
				new StructField(ROW_TABLE_NAME_FIELD, DataTypes.StringType, false, Metadata.empty()), //
				new StructField(ROW_KEY_VALUE_FIELD, DataTypes.StringType, true, Metadata.empty()), //
				new StructField(ROW_KEY_FIELD_FIELD, DataTypes.StringType, true, Metadata.empty()));
		int l = s.fields().length;
		return ds.map(r -> {
			Object[] vs = new Object[l];
			for (int i = 0; i < l - 1; i++)
				vs[i] = r.get(s.fields()[i].name());
			vs[l - 3] = r.table();
			vs[l - 2] = r.key();
			vs[l - 1] = r.keyField();
			return new GenericRowWithSchema(vs, s);
		}, RowEncoder.apply(s));
	}

	protected final Dataset<Row> rmap2rowDs0(Dataset<Rmap> ds) {
		if (schema.isEmpty()) throw new UnsupportedOperationException("No schema io could not map back to row.");
		StructType s = sparkSchema();
		int l = s.fields().length;
		return ds.map(r -> {
			Object[] vs = new Object[l];
			for (int i = 0; i < l - 1; i++)
				vs[i] = r.get(s.fields()[i].name());
			return new GenericRowWithSchema(vs, s);
		}, RowEncoder.apply(s));
	}

	protected final StructType sparkSchema(StructField... extras) {
		StructField[] sfs = new StructField[schema.size() + extras.length];
		for (int i = 0; i < schema.size(); i++)
			sfs[i] = new StructField(schema.get(i).name, Sparks.fieldType(schema.get(i).type), true, Metadata.empty());
		for (int i = 0; i < extras.length; i++)
			sfs[schema.size() + i] = extras[i];
		return new StructType(sfs);
	}

	// ====
}
