package net.butfly.albatis.hbase;

import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.spark.io.SparkSinkOutput;

//@Schema("hbase")
public class SparkHbaseOutput extends SparkSinkOutput {
	private static final long serialVersionUID = 5602542871208124774L;

	protected SparkHbaseOutput(SparkSession spark, URISpec targetUri) {
		super(spark, targetUri);
	}

	@Override
	public String format() {
		return "org.apache.spark.sql.execution.datasources.hbase";
	}

	@Override
	public Map<String, String> options() {
		Map<String, String> opts = super.options();
		opts.put(HBaseTableCatalog.tableCatalog(), "hBaseCatalog");
		opts.put(HBaseTableCatalog.newTable(), "5");
		return opts;
	}
}
