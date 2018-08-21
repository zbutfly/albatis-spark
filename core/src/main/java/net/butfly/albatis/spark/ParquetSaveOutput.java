package net.butfly.albatis.spark;

import static net.butfly.albatis.spark.impl.Sparks.SchemaSupport.byTable;

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
	public Map<String, String> options() {
		return Maps.of("path", root);
	}

	@Override
	public void enqueue(Sdream<Rmap> s) {
		if (!(s instanceof DSdream)) throw new UnsupportedOperationException("Can only save dataset");
		if (schemaAll().size() > 1) byTable(((DSdream) s).ds, this::write);
		else write(table().name, ((DSdream) s).ds);
	}

	protected void write(String t, Dataset<Row> ds) {
		Map<String, String> opts = options();
		opts.put("path", opts.remove("path") + t);
		long n = System.currentTimeMillis();
		try (WriteHandler w = WriteHandler.of(ds)) {
			w.save(format(), opts);
		} finally {
			logger().trace(() -> "Table split saved in " + (System.currentTimeMillis() - n) + " ms.");
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
