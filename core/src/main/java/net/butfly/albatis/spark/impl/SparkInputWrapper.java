package net.butfly.albatis.spark.impl;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.IO;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.io.Wrapper;
import net.butfly.albatis.spark.SparkInput;

public final class SparkInputWrapper extends SparkInput<Rmap> implements Wrapper<SparkInput<Rmap>> {
	private static final long serialVersionUID = 5957738224117308018L;
	private final SparkInput<?> base;

	public SparkInputWrapper(SparkInput<?> s, Dataset<Rmap> ds) {
		super(s.spark, s.targetUri);
		this.base = s;
		vals(ds);
	}

	@Override
	protected Dataset<Row> load() {
		return null;
	}

	@Override
	public Map<String, String> options() {
		return base.options();
	}

	@Override
	public <BB extends IO> BB bases() {
		return Wrapper.bases(base);
	}

	@Override
	public Map<String, TableDesc> schemaAll() {
		return Maps.of();
	}
}
