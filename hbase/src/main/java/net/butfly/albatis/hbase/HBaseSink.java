package net.butfly.albatis.hbase;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.apache.spark.sql.execution.streaming.Sink;

import net.butfly.albacore.utils.collection.Maps;
import scala.collection.Map;
import scala.runtime.AbstractFunction0;

public class HBaseSink implements Sink {
	private final String hbaseCatalog;

	public HBaseSink(Map<String, String> options) {
		hbaseCatalog = options.getOrElse("hbasecat", new AbstractFunction0<String>() {
			@Override
			public String apply() {
				return "";
			}
		});
	}

	@Override
	public void addBatch(long batchId, Dataset<Row> ds) {
		// val df = data.sparkSession.createDataFrame(data.rdd, data.schema)
		java.util.Map<String, String> opts = Maps.of(HBaseTableCatalog.tableCatalog(), hbaseCatalog, HBaseTableCatalog.newTable(), "5");
		ds.write().options(opts).format("org.apache.spark.sql.execution.datasources.hbase").save();
	}
}
