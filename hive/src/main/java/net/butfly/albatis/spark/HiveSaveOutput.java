package net.butfly.albatis.spark;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.Schemas;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.output.SparkSinkSaveOutput;
import net.butfly.albatis.spark.output.WriteHandler;
import net.butfly.albatis.spark.util.DSdream;

@Schema("hive")
public class HiveSaveOutput extends SparkSinkSaveOutput {
	private static final long serialVersionUID = 2452118954794960617L;
	private final String defdb;

	public HiveSaveOutput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
		String[] segs = targetUri.getPath().split("/");
		if (segs.length > 0) defdb = segs[0];
		else defdb = null;
	}

	@Override
	public void enqueue(Sdream<Rmap> s) {
		if (!(s instanceof DSdream)) throw new UnsupportedOperationException("Can only save dataset");
		DSdream d = (DSdream) s;
		String htbl = null == defdb && d.table.indexOf('.') >= 0 ? d.table : defdb + "." + d.table;
		String fieldNameList = String.join(", ", spark.sql("describe " + htbl).map(r -> r.getAs("col_name"), Encoders.STRING())
				.collectAsList());
		String sql = "insert into table " + htbl + " select " + fieldNameList + " from _to_save_to_hive_" + htbl;
		d.ds.createOrReplaceTempView("_to_save_to_hive_" + htbl.replaceAll("\\.", "_"));
		logger().info("Execute sql to insert hive [" + d.table + "]: " + sql);
		long now = System.currentTimeMillis();
		spark.sql(sql);
		logger().info("Execute sql to insert hive [" + d.table + "] finished in [" + (System.currentTimeMillis() - now) + " ms].");
	}

	protected void write(String t, Dataset<Row> ds) {
		long n = System.currentTimeMillis();
		logger().error(Schemas.row2rmap(ds.first()).toString());
		Map<String, String> opts = options(t);
		try (WriteHandler w = WriteHandler.of(ds)) {
			w.save(format(), opts);
		} finally {
			logger().info("Table [" + t + ": " + ds.toString() + "] saved [" + opts.get("path") + "] in " + (System.currentTimeMillis() - n)
					+ " ms.");
		}
	}
}
