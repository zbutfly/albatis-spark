package com.hzcominfo.dataggr.spark.io;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Output;

public abstract class SparkIO {
	@SuppressWarnings("rawtypes")
	private static final Map<String, Class<? extends SparkInputBase>> ADAPTER_INPUT = scan(SparkInputBase.class);
	@SuppressWarnings("rawtypes")
	private static final Map<String, Class<? extends SparkOutput>> ADAPTER_OUTPUT = scan(SparkOutput.class);

	protected SparkSession spark;
	protected JavaSparkContext jsc;
	protected URISpec targetUri;

	public SparkIO() {}

	protected SparkIO(SparkSession spark, URISpec targetUri) {
		super();
		this.spark = spark;
		this.jsc = new JavaSparkContext(spark.sparkContext());
		this.targetUri = targetUri;
	}

	protected abstract Map<String, String> options();

	protected String format() {
		return null;
	}

	protected abstract String schema();

	public static <V, O extends Output<V>> O output(SparkSession spark, URISpec uri) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<O> c = (Class<O>) ADAPTER_OUTPUT.get(s);
			if (null == c) break;
			else try {
				return c.getConstructor(SparkSession.class, URISpec.class).newInstance(spark, uri);
			} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	public static <V, I extends Input<V>> I input(SparkSession spark, URISpec uri) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<I> c = (Class<I>) ADAPTER_INPUT.get(s);
			if (null == c) break;
			else try {
				return c.getConstructor(SparkSession.class, URISpec.class).newInstance(spark, uri);
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
		for (Class<? extends C> c : Reflections.getSubClasses(parentClass))
			try {
				if (!Modifier.isAbstract(c.getModifiers()) && //
						!SparkIOLess.class.isAssignableFrom(c)) for (String s : c.newInstance().schema().split(","))
					map.put(s, c);
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		return map;
	}
}
