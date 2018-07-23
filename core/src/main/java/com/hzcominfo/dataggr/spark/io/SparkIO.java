package com.hzcominfo.dataggr.spark.io;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.hzcominfo.dataggr.spark.join.SparkJoinInput;
import com.hzcominfo.dataggr.spark.plugin.SparkPluginInput;
import com.hzcominfo.dataggr.spark.util.Maps;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.Reflections;

public abstract class SparkIO {
	private static final Map<String, Class<? extends SparkInput>> ADAPTER_INPUT = scan(SparkInput.class);
	private static final Map<String, Class<? extends SparkOutput>> ADAPTER_OUTPUT = scan(SparkOutput.class);

	protected SparkSession spark;
	protected JavaSparkContext jsc;
	protected URISpec targetUri;

	public SparkIO() {
	}

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

	public static <O extends SparkOutput> O output(SparkSession spark, URISpec uri) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<O> c = (Class<O>) ADAPTER_OUTPUT.get(s);
			if (null == c)
				break;
			else
				try {
					return c.getConstructor(SparkSession.class, URISpec.class).newInstance(spark, uri);
				} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
						| IllegalArgumentException | InvocationTargetException e) {
					throw new RuntimeException(e);
				}
		}
		throw new RuntimeException("No matched adapter with scheme: " + s);
	}

	public static <I extends SparkInput> I input(SparkSession spark, URISpec uri) {
		String s = uri.getScheme();
		while (!s.isEmpty()) {
			@SuppressWarnings("unchecked")
			Class<I> c = (Class<I>) ADAPTER_INPUT.get(s);
			if (null == c)
				break;
			else
				try {
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
				if (SparkJoinInput.class.isAssignableFrom(c) || SparkPluginInput.class.isAssignableFrom(c))
					continue;
				for (String s : c.newInstance().schema().split(","))
					map.put(s, c);
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		return map;
	}
}
