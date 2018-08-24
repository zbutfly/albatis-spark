package net.butfly.albatis.spark.impl;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.IO;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.SparkInput;

public abstract class SparkIO implements IO, Serializable {
	private static final long serialVersionUID = 3265459356239387878L;
	private static final Map<Class<? extends IO>, Map<String, Class<? extends SparkIO>>> ADAPTERS //
			= Maps.of(Input.class, Maps.of(), Output.class, Maps.of());
	static {
		scan();
	}

	public final SparkSession spark;
	public final URISpec targetUri;

	protected SparkIO(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super();
		this.spark = spark;
		this.targetUri = targetUri;

		if (table.length > 0) schema(table);
		// else if (null != targetUri && null != targetUri.getFile()) schema(TableDesc.dummy(targetUri.getFile()));
	}

	public String format() {
		return null;
	}

	public static <V, O extends Output<V>> O output(SparkSession spark, URISpec uri, TableDesc... table) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<O> cls = (Class<O>) ADAPTERS.get(Output.class).get(s);
			if (null == cls) {
				int c = s.lastIndexOf(":");
				if (c >= 0) s = s.substring(0, c);
				else break;
			} else try {
				return cls.getConstructor(SparkSession.class, URISpec.class, TableDesc[].class).newInstance(spark, uri, table);
			} catch (SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	public static <V, I extends SparkInput<Rmap>> I input(SparkSession spark, URISpec uri, TableDesc... table) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<I> cls = (Class<I>) ADAPTERS.get(Input.class).get(s);
			if (null == cls) {
				int c = s.lastIndexOf(":");
				if (c >= 0) s = s.substring(0, c);
				else break;
			} else try {
				return cls.getConstructor(SparkSession.class, URISpec.class, TableDesc[].class).newInstance(spark, uri, table);
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

	public TableDesc table() {
		Map<String, TableDesc> all = schemaAll();
		if (all.isEmpty()) //
			throw new RuntimeException("No table defined for spark i/o.");
		TableDesc a = all.values().iterator().next();
		if (all.size() > 1) Logger.getLogger(this.getClass()).warn("Multiple tables defined [" + all
				+ "] for spark i/o, now only support the first: [" + a + "].");
		return a;
	}

	public TableDesc[] tables() {
		return schemaAll().values().toArray(new TableDesc[0]);
	}

	@Override
	public int features() {
		return IO.super.features() | IO.Feature.SPARK;
	}
}
