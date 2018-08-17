package net.butfly.albatis.spark;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.output.SparkSaveOutput;

@Schema({ "hdfs", "file" })
public class ParquetOutput extends SparkSaveOutput {
	private static final long serialVersionUID = -5643925927378821988L;
	private final String format;
	private final String root;

	public ParquetOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
		String[] schemas = targetUri.getScheme().split(":");
		if (schemas.length > 1) format = schemas[1];
		else format = "parquet";
		String path = targetUri.getPath();
		char c = path.charAt(1);
		if ('.' == c || '~' == c) path = path.substring(1);
		if (!path.endsWith("/")) path += "/";
		path += table[0];
		// if (!path.endsWith("/")) path += "/";
		root = path;
		if (tables.length != 1) throw new IllegalArgumentException("ParquetOutput need one table, can be =Expression");
	}

	@Override
	public String format() {
		return format;
	}

	@Override
	public Map<String, String> options() {
		return Maps.of("path", root);
	}

	@Override
	public void enqueue(Sdream<Rmap> r) {
		super.enqueue(r);
	}
}
