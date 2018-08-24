package net.butfly.albatis.spark;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.output.SparkSinkSaveOutput;
import net.butfly.albatis.spark.output.WriteHandler;
import net.butfly.albatis.spark.util.DSdream;
import static net.butfly.albatis.spark.impl.Sparks.alias;

@Schema({ "hdfs", "file" })
public class ParquetSaveOutput extends SparkSinkSaveOutput {
	private static final long serialVersionUID = 2452118954794960617L;
	private final String format;
	private final String root;

	public ParquetSaveOutput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
		String[] schemas = targetUri.getScheme().split(":");
		if (schemas.length > 1) format = schemas[1];
		else format = "parquet";

		root = path();
	}

	@Override
	public String format() {
		return format;
	}

	@Override
	public Map<String, String> options(String table) {
		return Maps.of("path", root + table);
	}

	@Override
	public void enqueue(Sdream<Rmap> s) {
		if (!(s instanceof DSdream)) throw new UnsupportedOperationException("Can only save dataset");
		DSdream d = (DSdream) s;
		write(d.ds);
	}

	protected void write(Dataset<Row> ds) {
		long n = System.currentTimeMillis();
		try (WriteHandler w = WriteHandler.of(ds)) {
			w.save(format(), options(alias(ds)));
		} finally {
			logger().info("Table [" + t + ": " + ds.toString() + "] saved in " + (System.currentTimeMillis() - n) + " ms.");
		}
	}

	private String path() {
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
		return path;
	}
}
