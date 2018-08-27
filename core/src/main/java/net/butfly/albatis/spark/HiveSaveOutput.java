package net.butfly.albatis.spark;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.output.SparkSinkSaveOutput;
import net.butfly.albatis.spark.output.WriteHandler;
import net.butfly.albatis.spark.util.DSdream;

@Schema({ "hdfs", "file" })
public class HiveSaveOutput extends SparkSinkSaveOutput {
	private static final long serialVersionUID = 2452118954794960617L;

	public HiveSaveOutput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
		System.setProperty("albatis.spark.split", "-1");// not support split, target path could not be write twice.
	}

	@Override
	public void enqueue(Sdream<Rmap> s) {
		if (!(s instanceof DSdream)) throw new UnsupportedOperationException("Can only save dataset");
		DSdream d = (DSdream) s;
		write(d.table, d.ds);
	}

	protected void write(String t, Dataset<Row> ds) {
		long n = System.currentTimeMillis();
		Map<String, String> opts = options(t);
		try (WriteHandler w = WriteHandler.of(ds)) {
			w.save(format(), opts);
		} finally {
			logger().info("Table [" + t + ": " + ds.toString() + "] saved [" + opts.get("path") + "] in " + (System.currentTimeMillis() - n)
					+ " ms.");
		}
	}
}
