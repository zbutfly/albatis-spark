package net.butfly.albatis.spark.io;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albatis.io.Output;

public abstract class SparkIO implements Serializable {
	private static final long serialVersionUID = 3265459356239387878L;
	@SuppressWarnings("rawtypes")
	private static final Map<String, Class<? extends SparkInputBase>> ADAPTER_INPUT = scan(SparkInputBase.class);
	@SuppressWarnings("rawtypes")
	private static final Map<String, Class<? extends SparkOutput>> ADAPTER_OUTPUT = scan(SparkOutput.class);

	public final SparkSession spark;
	private transient JavaSparkContext jsc;
	public final URISpec targetUri;
	public final String[] tables;

	protected SparkIO(SparkSession spark, URISpec targetUri, String... table) {
		super();
		this.spark = spark;
		this.targetUri = targetUri;
		String[] t;
		if (table.length > 0) t = table;
		else if (null != targetUri.getFile()) t = new String[] { targetUri.getFile() };
		else t = new String[0];
		tables = t;
	}

	protected JavaSparkContext jsc() {
		return null == jsc ? (jsc = new JavaSparkContext(spark.sparkContext())) : jsc;
	}

	protected abstract Map<String, String> options();

	protected String format() {
		return null;
	}

	public static <V, O extends Output<V>> O output(SparkSession spark, URISpec uri, String... table) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<O> c = (Class<O>) ADAPTER_OUTPUT.get(s);
			if (null == c) break;
			else try {
				return c.getConstructor(SparkSession.class, URISpec.class).newInstance(spark, uri, table);
			} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	public static <V, I extends SparkInput> I input(SparkSession spark, URISpec uri, String... table) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<I> c = (Class<I>) ADAPTER_INPUT.get(s);
			if (null == c) break;
			else try {
				return c.getConstructor(SparkSession.class, URISpec.class, String[].class).newInstance(spark, uri, table);
			} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
					| IllegalArgumentException e) {
				throw new RuntimeException(e);
			} catch (InvocationTargetException e) {
				throw new RuntimeException(e.getTargetException());
			}
		}
		throw new RuntimeException("No matched adapter with scheme: " + s);
	}

	private static <C extends SparkIO> Map<String, Class<? extends C>> scan(Class<C> parentClass) {
		Map<String, Class<? extends C>> map = Maps.of();
		for (Class<? extends C> c : Reflections.getSubClasses(parentClass)) {
			Schema a = c.getAnnotation(Schema.class);
			if (null != a) for (String s : c.getAnnotation(Schema.class).value())
				map.put(s, c);
		}
		return map;
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public static @interface Schema {
		String[] value();
	}

	protected String table() {
		// String[] tables = tables.getValue();
		if (null == tables || tables.length == 0) //
			throw new RuntimeException("No table defined for spark i/o.");
		if (tables.length > 1) Logger.getLogger(this.getClass()).warn("Multiple tables defined [" + tables
				+ "] for spark i/o, now only support the first: [" + tables[0] + "].");
		return tables[0];
	}
}
