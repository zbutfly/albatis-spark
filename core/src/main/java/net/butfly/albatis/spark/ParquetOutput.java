package net.butfly.albatis.spark;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.output.SparkSaveOutput;

@Schema({ "hdfs:basic", "file:basic" })
public class ParquetOutput extends SparkSaveOutput {
	private static final long serialVersionUID = -5643925927378821988L;
	private final String format;
	private final String root;

	public ParquetOutput(SparkSession spark, URISpec targetUri, String... table) {
		super(spark, targetUri, table);
		if (tables.length != 1) throw new IllegalArgumentException("ParquetOutput need one table, can be =Expression");
		String[] schemas = targetUri.getScheme().split(":");
		if (schemas.length > 2) format = schemas[2];
		else format = "parquet";

		root = parseLocal();
		// root = parse(schemas[0]);
		logger().info("Parquet (native) save to: " + root);
	}

	@Override
	public String format() {
		return format;
	}

	@Override
	public Map<String, String> options() {
		return Maps.of("path", root);
	}

	protected final String parse(String s) {
		switch (s) {
		case "file":
			return parseLocal();
		case "hdfs":
			return parseHdfs();
		default:
			throw new UnsupportedOperationException("schema of [" + targetUri + "] is not supported.");
		}
	}

	private String parseLocal() {
		String host = targetUri.getHost();
		String path = targetUri.getPath();
		if (!path.endsWith("/")) path += "/";
		switch (host) {
		case ".":
		case "~":
			path = host + path;
			break;
		case "":
			break;
		default:
			throw new UnsupportedOperationException("hosts can only be '.', '~' or empty.");
			// return parseHdfs();
		}
		path += table();
		return path;
	}

	private String parseHdfs() {
		String path = "hdfs://" + targetUri.getAuthority() + targetUri.getPath();
		if (!path.endsWith("/")) path += "/";
		path += table();
		return path;
	}
}
