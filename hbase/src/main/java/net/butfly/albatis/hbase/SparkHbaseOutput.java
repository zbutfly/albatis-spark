package net.butfly.albatis.hbase;

import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albatis.ddl.TableDesc;
import net.butfly.albatis.spark.output.SparkSaveOutput;

//@Schema("hbase")
public class SparkHbaseOutput extends SparkSaveOutput {
	private static final long serialVersionUID = 5602542871208124774L;

	protected SparkHbaseOutput(SparkSession spark, URISpec targetUri, TableDesc table) {
		super(spark, targetUri, table);
	}

	@Override
	public String format() {
		return "org.apache.spark.sql.execution.datasources.hbase";
	}

	@Override
	public Map<String, String> options(String table) {
		Map<String, String> opts = Maps.of();
		opts.put(HBaseTableCatalog.tableCatalog(), "hBaseCatalog");
		opts.put(HBaseTableCatalog.newTable(), "5");
		return opts;
	}
}
