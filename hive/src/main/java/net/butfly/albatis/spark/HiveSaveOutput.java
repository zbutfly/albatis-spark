package net.butfly.albatis.spark;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.albatis.spark.impl.Schemas;
import net.butfly.albatis.spark.impl.SparkConf;
import net.butfly.albatis.spark.impl.SparkIO.Schema;
import net.butfly.albatis.spark.output.SparkSinkSaveOutput;
import net.butfly.albatis.spark.output.WriteHandler;
import net.butfly.albatis.spark.util.DSdream;

@Schema("hive")
@SparkConf("spark.sql.catalogImplementation=hive")
@SparkConf("hive.exec.dynamic.partition.mode=nonstrict")
public class HiveSaveOutput extends SparkSinkSaveOutput {
	private static final long serialVersionUID = 2452118954794960617L;
	private final String defdb;
	private String mode;

	public HiveSaveOutput(SparkSession spark, URISpec targetUri, TableDesc... table) {
		super(spark, targetUri, table);
		String[] segs = targetUri.getPath().substring(1).split("/");
		if (segs.length > 1) defdb = segs[0];
		else defdb = null;
		this.mode = targetUri.getParameter("mode", "into");
	}

	@Override
	public void enqueue(Sdream<Rmap> s) {
		if (!(s instanceof DSdream)) throw new UnsupportedOperationException("Can only save dataset");
		DSdream d = (DSdream) s;
		String htbl = null == defdb || d.table.indexOf('.') >= 0 ? d.table : defdb + "." + d.table;
		List<String> dbFields = spark.sql("describe " + htbl).map(r -> r.getAs("col_name"), Encoders.STRING()).collectAsList();
		List<String> selFields = Colls.list();
		for (String dbf : dbFields) {
			if ('#' == dbf.charAt(0)) break;
			boolean found = false;
			for (String dsf : d.ds.schema().fieldNames())
				if (dsf.equalsIgnoreCase(dbf)) {
					selFields.add(dsf + " as " + dbf);
					found = true;
					break;
				}
			if (!found) {
				logger().warn("Database table [" + htbl + "] defined col [" + dbf + "], but not found in dataset, ignore by null as it.");
				selFields.add("null as " + dbf);
			}
		}
		String sql = "insert " + mode + " table " + htbl + " select\n\t" + String.join(", \n\t", selFields) + "\nfrom _to_save_to_hive_"
				+ htbl;
		String view = "_to_save_to_hive_" + htbl.replaceAll("\\.", "_");
		d.ds.createOrReplaceTempView(view);
		logger().info("Execute sql to insert hive table [" + htbl + "] from spark view [" + view + "]: \n" + sql);
		long now = System.currentTimeMillis();
		spark.sql(sql);
		logger().info("Execute sql to insert hive [" + htbl + "] finished in [" + (System.currentTimeMillis() - now) + " ms].");
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
